package zktq

import (
	"container/list"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const DEFAULT_WORKER_THREAD = 4

type masterContext struct {
	appName           string        //name of this application
	conn              *zk.Conn      //zookeeper connection descriptor
	masterL           sync.Mutex    //master context locker
	handler           chan struct{} //close when no longer master. won't init again once closed,
	isMaster          bool          //is master
	tasksUnfinished   []string      //task list of tasks unfinished on worker side(sorted)
					//tasksUnDistribute []string      //task list of tasks not distributed(sorted)
	workers           []string	//workers online
	idleWorkers       []string	//workers waiting task
	tasksBelong       map[string]string	//relationship of task belongs which worker(key is task, value is worker)
	working           map[string](map[string]bool)	//relationship of worker working on which tasks(key is worker, value is map of working tasks)

	//generate task with given last check point, returns tasks, this check point, and duration to next check point
	taskGenerator     func(lastCheckPoint string) (tasks []string, thisPoint string, sleepTime time.Duration)
}

type workerContext struct {
	appName    string //name of this application
	workerName string //worker name , with uuid
	conn       *zk.Conn //zookeeper connection descriptor

	workerL  *sync.Mutex//worker context locker
	handler  chan struct{}//close when no longer worker. won't init again once closed,
	isWorker bool//is worker

	workFunc        func(task string) error	//function of working
	tasksChan       chan string	//chan to distribute tasks
	tasksUnfinished []string	//list of pending tasks
	tasksList       *list.List	//list of tasks waiting for process, value is same as tasksUnfinished

	newTask *sync.Cond
	//newTask chan struct{}	//to notify thread waiting on new task, only notify when tasks length from zero to one
}

type ZKTaskQueue struct {
	appName       string
	taskGenerator func(lastCheckPoint string) (tasks []string, thisPoint string, sleepTime time.Duration)

	conn *zk.Conn
	c    <-chan zk.Event

	master *masterContext
	worker *workerContext

	l      sync.Mutex
	online bool
}

func workerName() string {
	hostname, _ := os.Hostname()
	uuid := md5.Sum([]byte(fmt.Sprintf("%s_%d", hostname, time.Now().Unix())))
	return hostname + "_" + hex.EncodeToString(uuid[:])
}

func NewZKTaskQueue(
zks string,
appName string,
worker func(task string) error,
taskGenerator func(lastCheckPoint string) (tasks []string, thisPoint string, sleepTime time.Duration),
) (*ZKTaskQueue, error) {
	servers := strings.Split(zks, ",")
	conn, eventc, err := zk.Connect(servers, time.Second*5)
	if err != nil {
		return nil, err
	}

	conn.Create(fmt.Sprintf("/%s/workers", appName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
	conn.Create(fmt.Sprintf("/%s/working", appName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
	conn.Create(fmt.Sprintf("/%s/tasks", appName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
	conn.Create(fmt.Sprintf("/%s/finished_tasks", appName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
	conn.Create(fmt.Sprintf("/%s/idle_workers", appName), []byte{0}, 0, zk.WorldACL(zk.PermAll))

	workerL := sync.Mutex{}

	result := &ZKTaskQueue{
		appName:       appName,
		taskGenerator: taskGenerator,

		conn: conn,
		c:    eventc,

		master: nil,
		worker: &workerContext{
			appName:    appName,
			workerName: workerName(),
			conn:       conn,

			workerL: &workerL ,
			handler:  make(chan struct{}),
			isWorker: true,

			workFunc:        worker,
			tasksChan:       make(chan string),
			tasksUnfinished: []string{},
			tasksList:       &list.List{},

			newTask: sync.NewCond(&workerL),
		},

		l:      sync.Mutex{},
		online: false,
	}

	time.AfterFunc(time.Second*5, result.masterLockLoop)
	go result.worker.workerWatchLoop()
	go result.aliveLoop()

	return result, nil
}

func (p *ZKTaskQueue) masterLockLoop() {
	for {

		lock := zk.NewLock(p.conn, fmt.Sprintf("/%s/master", p.appName), zk.WorldACL(zk.PermAll))
		err := lock.Lock()
		if err != nil {
			continue
		}

		p.l.Lock()
		p.master = &masterContext{
			appName: p.appName,
			conn:    p.conn,

			handler:           make(chan struct{}),
			isMaster:          true,
			tasksUnfinished:   []string{},
			//tasksUnDistribute: []string{},
			workers:           []string{},
			idleWorkers:       []string{},
			tasksBelong:       map[string]string{},
			working:           map[string](map[string]bool){},

			taskGenerator: p.taskGenerator,
		}
		p.master.handler = make(chan struct{})
		//close(p.worker.handler)
		p.l.Unlock()

		logrus.Infoln("[zktq] Be Master!!!!!!!!!!!!!!!!!!!!!!!")


		go p.master.masterWatchLoop()
		go p.master.genTasksLoop()
		p.master.clean()
		go p.master.cleanLoop()
		<-p.master.handler
		lock.Unlock()
	}
}

func (p *masterContext) clean() error {
	tasks, _, err := p.conn.Children(fmt.Sprintf("/%s/finished_tasks", p.appName))
	if err != nil {
		logrus.Errorln("[zktq] cleanLoop: get finished_tasks: ", err.Error())
		return err
	}

	for len(tasks) > 0 {
		l := len(tasks)
		if l > 1000 {
			l = 1000
		}

		ops := make([]interface{}, l*2)
		for i := 0; i < l; i++ {
			ops[i] = &zk.DeleteRequest{fmt.Sprintf("/%s/tasks/%s", p.appName, tasks[i]), 0}
			ops[i+l] = &zk.DeleteRequest{fmt.Sprintf("/%s/finished_tasks/%s", p.appName, tasks[i]), 0}
		}
		resp, err := p.conn.Multi(ops...)
		if err != nil {
			logrus.Errorln("[zktq] cleanLoop: remove finished tasks: ", err.Error(), resp)
			return err
		}

		tasks = tasks[l:]
	}

	return nil
}

func (p *masterContext) cleanLoop() {
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.clean()

		case <-p.handler:
			return
		}
	}
}

func (p *masterContext) lessTaskWorker() string {
	p.masterL.Lock()
	defer p.masterL.Unlock()
	/*var (
		wker string
		num  = 0x7fffffff
	)
	for worker, tasks := range p.working {
		if len(tasks) < num {
			num = len(tasks)
			wker = worker
		}
	}

	return wker*/

	if len(p.idleWorkers) == 0 {
		return ""
	}

	wker := p.idleWorkers[0]

	return wker
}

func (p *masterContext) genTasksLoop() {
	var data string
	var version int32

	bs, stat, err := p.conn.Get(fmt.Sprintf("/%s/checkPoint", p.appName))
	if err != nil {
		p.conn.Create(fmt.Sprintf("/%s/checkPoint", p.appName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
		version = 0
		data = ""
	} else {
		data = string(bs)
		version = stat.Version
	}

	for {
		if !p.isMaster {
			return
		}

		tasks, checkPoint, dur := p.taskGenerator(data)
		data = checkPoint

		t := time.NewTimer(dur)

		logrus.Debug("[zktq] task generator: ", tasks, checkPoint, dur)

		reqs := make([]interface{}, len(tasks))
		for i, task := range tasks {
			reqs[i] = &zk.CreateRequest{fmt.Sprintf("/%s/tasks/task_", p.appName), []byte(task), zk.WorldACL(zk.PermAll), zk.FlagSequence}
		}

		if !p.isMaster {
			return
		}
		_, err := p.conn.Multi(reqs...)
		if err != nil {
			//todo
		}

		if !p.isMaster {
			return
		}

		_, err = p.conn.Set(fmt.Sprintf("/%s/checkPoint", p.appName), []byte(checkPoint), version)
		if err != nil {
			logrus.Error("[zktq:master] error when set check point: ", err.Error())
		}
		version++

		<-t.C
	}
}

func (p *masterContext) distribute(task string) string {
	p.masterL.Lock()
	if p.tasksBelong[task] != "" {
		p.masterL.Unlock()
		return ""
	}
	p.masterL.Unlock()

	worker := p.lessTaskWorker()
	if worker == "" {
		return ""
	}

	_, err := p.conn.Create(fmt.Sprintf("/%s/working/%s/%s", p.appName, worker, task), []byte{0}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return ""
	}

	p.masterL.Lock()
	if p.working[worker] == nil {
		p.working[worker] = map[string]bool{}
	}
	p.working[worker][task] = true
	p.tasksBelong[task] = worker
	p.masterL.Unlock()

	logrus.Infoln("[zktq:master]", task, " -> ", worker)

	return worker
}

func (p *masterContext) masterDistributeOne() string {
	p.masterL.Lock()
	if len(p.idleWorkers) == 0 {
		p.masterL.Unlock()
		return ""
	}
	p.masterL.Unlock()

	for _, task := range p.tasksUnfinished {
		if worker := p.distribute(task) ; worker != "" {
			return worker
		}
	}

	return ""
}

func (p *masterContext) masterDistribute() {
	for p.masterDistributeOne() != "" {}
}

func compare(old []string, new []string) (fresh []string, retire []string) {
	oldm := map[string]bool{}
	newm := map[string]bool{}

	for _, o := range old {
		oldm[o] = true
	}

	for _, n := range new {
		newm[n] = true
	}

	fresh = []string{}
	retire = []string{}

	for _, n := range new {
		if !oldm[n] {
			fresh = append(fresh, n)
		}
	}

	for _, o := range old {
		if !newm[o] {
			retire = append(retire, o)
		}
	}

	return
}

func (p *masterContext) sTATE() {
	for {
		logrus.Debug("--------------------------------------------------")
		//logrus.Debugf("[zktq] [state] not finished tasks: [%v]", p.tasksUnfinished)
		//logrus.Debugf("[zktq] [state] not distributed tasks: [%v]", p.tasksUnDistribute)
		logrus.Debugf("[zktq] [state] now workers: [%v]", p.workers)
		logrus.Debugf("[zktq] [state] now idles: [%v]", p.idleWorkers)
		logrus.Debugf("[zktq] [state] task bolongs: [%v]", p.tasksBelong)
		logrus.Debugf("[zktq] [state] worker tasks: [%v]", p.working)

		time.Sleep(time.Second * 10)
	}
}

func (p *masterContext) masterUpdateWorkers(workers []string) (freshWorker []string, retireWorker []string) {
	logrus.Infoln("[zktq:master] now workers: ", workers)

	p.masterL.Lock()
	defer p.masterL.Unlock()

	freshWorker, retireWorker = compare(p.workers, workers)
	p.workers = workers

	defer logrus.Infof("[zktq:master] freshWorker:[%v], retireWorker:[%v]", freshWorker, retireWorker)

	for _, worker := range freshWorker {
		p.working[worker] = map[string]bool{}
	}

	for _, worker := range retireWorker {
		if p.working[worker] != nil {
			for task, _ := range p.working[worker] {
				delete(p.tasksBelong, task)
				//p.tasksUnDistribute = append(p.tasksUnDistribute, task) //roll back
				p.conn.Delete(fmt.Sprintf("/%s/working/%s/%s", p.appName, worker, task), 0)
			}
		}

		delete(p.working, worker)
	}

	//sort.Strings(p.tasksUnDistribute)
	return
}

func (p *masterContext) masterUpdateTasks(tasks []string) (freshTask []string, retireTask []string) {
	logrus.Infoln("[zktq:master] now tasks: ", tasks)
	p.masterL.Lock()
	defer p.masterL.Unlock()

	freshTask, retireTask = compare(p.tasksUnfinished, tasks)

	sort.Strings(freshTask)
	sort.Strings(retireTask)

	p.tasksUnfinished = tasks

	defer logrus.Infof("[zktq:master] freshTask:[%v], retireTask:[%v]", freshTask, retireTask)

	for _, task := range retireTask {
		delete(p.working[p.tasksBelong[task]], task)
		delete(p.tasksBelong, task)
	}

	sort.Strings(p.tasksUnfinished)

	return
}

/*
master loop, call this function when program become master.
doing master things, and return when not be master any more.
*/
func (p *masterContext) masterWatchLoop() {
	//go p.sTATE()
	logrus.Infoln("[zktq:master] start master watch loop")
	defer logrus.Infoln("[zktq:master] ending master watch loop")

	workers, _, event_workers, err := p.conn.ChildrenW(fmt.Sprintf("/%s/workers", p.appName))
	if err != nil {

	}
	p.masterUpdateWorkers(workers)

	tasks, _, event_tasks, err := p.conn.ChildrenW(fmt.Sprintf("/%s/tasks", p.appName))
	if err != nil {
		//todo:
	}
	p.masterUpdateTasks(tasks)

	idle_workers, _, event_idle_workers, err := p.conn.ChildrenW(fmt.Sprintf("/%s/idle_workers", p.appName))
	if err != nil {
		//todo:
	}
	p.masterL.Lock()
	p.idleWorkers = idle_workers
	p.masterL.Unlock()

	for {
		go p.masterDistribute()
		select {
		case e := <-event_tasks:
			if e.Type == zk.EventNotWatching {
				return
			}

			tasks, _, event_tasks, err = p.conn.ChildrenW(fmt.Sprintf("/%s/tasks", p.appName))
			if err != nil {

			}
			p.masterUpdateTasks(tasks)
		case e := <-event_workers:
			if e.Type == zk.EventNotWatching {
				return
			}

			workers, _, event_workers, err = p.conn.ChildrenW(fmt.Sprintf("/%s/workers", p.appName))
			if err != nil {

			}
			p.masterUpdateWorkers(workers)
		case e := <-event_idle_workers:
			if e.Type == zk.EventNotWatching {
				return
			}

			idle_workers, _, event_idle_workers, err = p.conn.ChildrenW(fmt.Sprintf("/%s/idle_workers", p.appName))
			if err != nil {

			}

			p.masterL.Lock()
			p.idleWorkers = idle_workers
			p.masterL.Unlock()
		case <-p.handler:
			return
		}
	}
}

func (p *workerContext) workLoop() {
	for {
		select {
		case taskName := <-p.tasksChan: //if not worker any more, should set p.tasks nil, to block
			logrus.Info("[zktq:<worker>] workLoop: start woring on task: ", taskName)

			bs, _, err := p.conn.Get(fmt.Sprintf("/%s/tasks/%s", p.appName, taskName))
			if err != nil {
				continue
				//todo:
			}
			task := string(bs)

			err = p.workFunc(task)

			if err != nil {
				ops := make([]interface{}, 2)
				ops[0] = &zk.CreateRequest{fmt.Sprintf("/%s/tasks/task_", p.appName), []byte(task), zk.WorldACL(zk.PermAll), zk.FlagSequence}
				ops[1] = &zk.DeleteRequest{fmt.Sprintf("/%s/tasks/%s", p.appName, taskName), 0}
				_, err := p.conn.Multi(ops...)
				if err != nil {
					logrus.Errorln("[zktq] can't requeue task: ", err.Error())
					//todo:
				}
			} else {
				_, err := p.conn.Create(fmt.Sprintf("/%s/finished_tasks/%s", p.appName, taskName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
				if err != nil {
					logrus.Errorln("[zktq] can't delete task: ", err.Error())
					//todo:
				}
			}
			_ = p.conn.Delete(fmt.Sprintf("/%s/working/%s/%s", p.appName, p.workerName, taskName), 0)
		//todo: errors
		case <-p.handler:
			return
		}
	}
}

func (p *workerContext) sendTaskLoop() {
	for {
		p.workerL.Lock()
		if !p.isWorker {
			p.workerL.Unlock()
			return
		}

		if p.tasksList.Len() == 0 {
			p.newTask.Wait()
		}

		if !p.isWorker {
			p.workerL.Unlock()
			return
		}

		if p.tasksList.Len() == 0 {
			p.workerL.Unlock()
			continue
		}

		firstTask := p.tasksList.Front().Value.(string)
		p.tasksList.Remove(p.tasksList.Front())
		p.workerL.Unlock()

		select {
		case p.tasksChan <- firstTask:
		case <-p.handler:
			close(p.tasksChan)
			return
		}
	}
}

func (p *workerContext) updateTasks(tasks []string) {
	logrus.Info("[zktq:<worker>]updateTasks: ", tasks)
	defer logrus.Info("[zktq:<worker>] end updateTasks")
	sort.Strings(tasks)

	p.workerL.Lock()
	defer p.workerL.Unlock()
	fresh, _ := compare(p.tasksUnfinished, tasks)

	logrus.Info("[zktq:<worker>] fresh tasks: ", fresh)

	noTaskBefore := p.tasksList.Len() == 0

	for _, task := range fresh {
		p.tasksList.PushBack(task)
	}

	p.tasksUnfinished = tasks

	if p.tasksList.Len() < 2 {
		logrus.Warn("[zktq:<worker>] create idle_workers")
		_, _ = p.conn.Create(fmt.Sprintf("/%s/idle_workers/%s", p.appName, p.workerName), []byte{0}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	}

	if p.tasksList.Len() >= 10 {
		logrus.Warn("[zktq:<worker>] remove idle_workers")
		_ = p.conn.Delete(fmt.Sprintf("/%s/idle_workers/%s", p.appName, p.workerName), 0)
	}

	if noTaskBefore && len(fresh) != 0 {
		p.newTask.Broadcast()
	}

}

func (p *workerContext) workerWatchLoop() {
	for i := 0; i < DEFAULT_WORKER_THREAD; i++ {
		go p.workLoop()
	}
	go p.sendTaskLoop()

	_, err := p.conn.Create(fmt.Sprintf("/%s/working/%s", p.appName, p.workerName), []byte{0}, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		//todo
	}

	_, err = p.conn.Create(fmt.Sprintf("/%s/workers/%s", p.appName, p.workerName), []byte{0}, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		//todo
	}

	tasks, _, event_tasks, err := p.conn.ChildrenW(fmt.Sprintf("/%s/working/%s", p.appName, p.workerName))
	if err != nil {
		logrus.Errorf("[zktq:<worker>] can't watch at /%s/working/%s: error: %s", p.appName, p.workerName, err.Error())
		//todo
	}

	for {
		p.updateTasks(tasks)

		select {
		case e := <-event_tasks:
			if e.Type == zk.EventNotWatching {
				logrus.Errorf("[zktq:<worker>] EventNotWatching, error: %s", e.Err.Error())
				//todo
				return
			}

			tasks, _, event_tasks, err = p.conn.ChildrenW(fmt.Sprintf("/%s/working/%s", p.appName, p.workerName))
			if err != nil {
				//todo
			}
		case <-p.handler:
			return
		}
	}
}

func (p *ZKTaskQueue) aliveLoop() {
	for {
		event := <-p.c
		switch true {
		case event.Type == zk.EventSession && event.State == zk.StateExpired:
			p.l.Lock()
			if p.online {
				p.online = false
				if p.master != nil {
					p.master.masterL.Lock()
					p.master.isMaster = false
					close(p.master.handler)
					p.master.masterL.Unlock()
					p.master = nil
				}
				if p.worker != nil {
					p.worker.workerL.Lock()
					p.worker.isWorker = false
					close(p.worker.handler)
					p.worker.newTask.Broadcast()
					p.worker.workerL.Unlock()
				}
			}
			p.l.Unlock()
		case event.Type == zk.EventSession && event.State == zk.StateHasSession:
			p.l.Lock()
			if !p.online {
				p.online = true
				if !p.worker.isWorker {
					p.worker.workerName = workerName()
					p.worker.isWorker = true
					p.worker.handler = make(chan struct{})
					go p.worker.workerWatchLoop()
				}
			}
			p.l.Unlock()
		}
	}
}
