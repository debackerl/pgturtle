package main

import (
	"gopkg.in/alecthomas/kingpin.v1"
	"github.com/jackc/pgx"
	"github.com/naoina/toml"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"io/ioutil"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	appCmdLine = kingpin.New("pgrunner", "Task queue for PostgreSQL.")
	configPathArg = appCmdLine.Flag("config", "Path to configuration file.").Required().String()
	dbConfig pgx.ConnConfig
	interrupt chan os.Signal
	stop int32
	db *pgx.Conn
)

var config struct {
	Postgres struct {
		Dsn string
		UpdatesChannelName string
		TableName string
	}
	Workers map[string]Worker
}

type Worker struct {
	Command string
	Arguments []string
	TimeoutSecs int
}

func loadConfig(path string) {
	// default values
	config.Postgres.UpdatesChannelName = "task"
	config.Postgres.TableName = "tasks"
	
	f, err := os.Open(path)
	if err != nil {
		log.Fatalln("Cannot open configuration file:", err)
	}
	defer f.Close()
	
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalln("Cannot read configuration file:", err)
	}
	
	if err := toml.Unmarshal(buf, &config); err != nil {
		log.Fatalln("Cannot decode configuration file:", err)
	}
}

func main() {
	var err error
	
	kingpin.MustParse(appCmdLine.Parse(os.Args[1:]))
	
	loadConfig(*configPathArg)
	
	dbConfig, err = pgx.ParseDSN(config.Postgres.Dsn)
	
	if err != nil {
		log.Fatalln("Could not parse DSN: ", err)
	}
	
	interrupt = make(chan os.Signal, 1)
	go signalHanlder()
	
	for {
		checkConnection()
		
		_, err := db.WaitForNotification(time.Minute)
		if err != nil && err != pgx.ErrNotificationTimeout {
			log.Println("Could not wait for notification:", err)
		}
		
		process()
	}
}

func signalHanlder() {
	<-interrupt
	signal.Stop(interrupt)
	close(interrupt)
	
	atomic.StoreInt32(&stop, 1)
}

func process() {
	var err error
	var ok bool
	
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	
	ok, err = fetch_task()
	for ok {
		if atomic.LoadInt32(&stop) == 1 {
			os.Exit(0)
		}
		ok, err = fetch_task()
	}
	
	if err != nil {
		log.Println(err)
		db.Close()
	}
}

// returns true if a task has been processed
func fetch_task() (bool, error) {
	var err error
	var tx *pgx.Tx
	var rows *pgx.Rows
	
	if tx, err = db.Begin(); err != nil {
		return false, fmt.Errorf("Could not open transaction: ", err.Error())
	}
	
	defer tx.Rollback()
	
	if rows, err = tx.Query(`SELECT task_id,worker,parameters FROM ` + quoteIdentifier(config.Postgres.TableName) + ` WHERE status='queued' ORDER BY insert_time LIMIT 1`); err != nil {
		return false, fmt.Errorf("Could not query for tasks: ", err.Error())
	}
	
	defer rows.Close()
	
	if rows.Next() {
		var task_id int64
		var worker string
		var parameters []byte
		
		if err := rows.Scan(&task_id, &worker, &parameters); err != nil {
			return false, fmt.Errorf("Could not retrieve columns: ", err.Error())
		}
		
		status, result := work(worker, parameters)
		
		tx.Exec(`UPDATE ` + quoteIdentifier(config.Postgres.TableName) + ` SET status=$2,result=$3 WHERE task_id=$1`, task_id, status, result)
	} else {
		return false, nil
	}
	
	if err = tx.Commit(); err != nil {
		return false, fmt.Errorf("Could not commit transaction: ", err.Error())
	}
	
	return true, nil
}

func work(worker string, parameters []byte) (status string, result []byte) {
	var err error
	var w Worker
	var ok bool
	var timer *time.Timer
	var b bytes.Buffer
	
	status = "failed"
	
	if w, ok = config.Workers[worker]; !ok {
		result = []byte("Unknown worker.")
		return
	}
	
	cmd := exec.Command(w.Command, w.Arguments...)
	
	cmd.Stdout = &b
	cmd.Stderr = &b
	
	stdin, err := cmd.StdinPipe()
	if err != nil {
		result = []byte("Could not open standard input.")
		return
	}
	
	cmd.Start()
	
	if w.TimeoutSecs > 0 {
		timer = time.AfterFunc(time.Duration(w.TimeoutSecs) * time.Second, func() {
			err := cmd.Process.Kill()
			if err != nil {
				panic(err)
			}
		})
	}
	
	if _, err := stdin.Write(parameters); err != nil {
		result = []byte("Could not write to standard input.")
		return
	}
	
	if err := stdin.Close(); err != nil {
		result = []byte("Could not close standard input.")
		return
	}
	
	err = cmd.Wait()
	
	if timer != nil {
		timer.Stop()
	}
	
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			// not an exit error issue, so something else happened, like a timeout
			result = []byte(err.Error())
			return
		}
	} else {
		status = "completed"
	}
	
	result = b.Bytes()
	return
}

func checkConnection() {
	var err error
	
	for !db.IsAlive() {
		if db, err = pgx.Connect(dbConfig); err != nil {
			log.Println("Could not open connection to database:", err)
		} else if err = db.Listen(quoteIdentifier(config.Postgres.UpdatesChannelName)); err != nil {
			log.Println("Could not listen to channel:", err)
			db.Close()
		} else {
			process()
			break
		}
		
		time.Sleep(time.Second)
	}
}

func quoteIdentifier(name string) string {
	if end := strings.IndexRune(name, 0); end > -1 {
		name = name[:end]
	}
	
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
