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
	"strconv"
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
	quotedTableName string
	fetchQuery string
	fetchQueryArgs pgx.QueryArgs
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
	
	quotedTableName = quoteIdentifier(config.Postgres.TableName)
	
	var q bytes.Buffer
	q.WriteString(`UPDATE ` + quotedTableName + ` a SET begin_time=now(), status='running' FROM (SELECT task_id FROM ` + quotedTableName + ` WHERE status = 'queued' OR (status = 'running' AND begin_time <= now() - interval '10 seconds' - (CASE worker`)
	for name, worker := range config.Workers {
		q.WriteString(" WHEN ")
		q.WriteString(fetchQueryArgs.Append(name))
		q.WriteString(" THEN ")
		q.WriteString(strconv.Itoa(worker.TimeoutSecs))
	}
	q.WriteString(` ELSE NULL END * interval '1 second')) ORDER BY insert_time LIMIT 1) b WHERE a.task_id = b.task_id RETURNING a.task_id,a.worker,a.parameters`)
	
	fetchQuery = q.String()
}

func main() {
	var err error
	
	log.SetOutput(os.Stdout)
	
	kingpin.MustParse(appCmdLine.Parse(os.Args[1:]))
	
	loadConfig(*configPathArg)
	
	dbConfig, err = pgx.ParseDSN(config.Postgres.Dsn)
	
	if err != nil {
		log.Fatalln("Could not parse DSN: ", err)
	}
	
	interrupt = make(chan os.Signal, 1)
	go signalHanlder()
	
	log.Println("pgturtle started.")
	
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

func process() bool {
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
	
	return err == nil
}

// returns true if a task has been processed
func fetch_task() (bool, error) {
	var err error
	var rows *pgx.Rows
	
	if rows, err = db.Query(fetchQuery, fetchQueryArgs...); err != nil {
		return false, fmt.Errorf("Could not query for tasks: ", err.Error())
	}
	
	if rows.Next() {
		var task_id int64
		var worker string
		var parameters []byte
		
		if err := rows.Scan(&task_id, &worker, &parameters); err != nil {
			rows.Close()
			return false, fmt.Errorf("Could not retrieve columns: ", err.Error())
		}
		
		rows.Close()
		
		log.Println("Starting worker:", worker)
		status, result := work(worker, parameters)
		log.Println("Result:", status)
		
		if _, err := db.Exec(`UPDATE ` + quotedTableName + ` SET status=$2,result=$3,end_time=now() WHERE task_id=$1`, task_id, status, result); err != nil {
			return false, fmt.Errorf("Could not update task: ", err.Error())
		}
	} else {
		rows.Close()
		return false, nil
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
	
	for db == nil || !db.IsAlive() {
		if db, err = pgx.Connect(dbConfig); err != nil {
			log.Println("Could not open connection to database:", err)
		} else if err = db.Listen(quoteIdentifier(config.Postgres.UpdatesChannelName)); err != nil {
			log.Println("Could not listen to channel:", err)
			db.Close()
		} else if process() {
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
