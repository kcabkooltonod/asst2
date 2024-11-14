#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    num_threads_ = num_threads;
    threads_pool_ = new std::thread[num_threads];
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete [] threads_pool_;
}

void TaskSystemParallelSpawn::runThread(IRunnable* runnable, int num_total_tasks, std::atomic_int& cur_task) {
    while(1) {
        int task_id = cur_task.fetch_add(1);

        if(task_id >= num_total_tasks) break;
        runnable->runTask(task_id, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::atomic_int cur_task(0);
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i] = std::thread(&TaskSystemParallelSpawn::runThread, this, runnable, num_total_tasks, std::ref(cur_task));
    }
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    num_threads_ = num_threads;
    threads_pool_ = new std::thread[num_threads];
    shut_down_ = false;

    runnable_  = nullptr;
    mutex_ = new std::mutex();
    tasks_complete_ = 0;
    num_total_tasks_ = 0;
    tasks_left_ = 0;

    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runThreadSpinning, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    shut_down_ = true;
    for (int i = 0; i < num_threads_; i++){
        threads_pool_[i].join();
    }
    delete [] threads_pool_;
    delete mutex_;
}

void TaskSystemParallelThreadPoolSpinning::runThreadSpinning() {
    while(true) {
        if(shut_down_) break;
        int task_id;
        mutex_->lock();
        task_id = num_total_tasks_ - tasks_left_;
        // if(task_id < num_total_tasks_)
        tasks_left_--;
        mutex_->unlock();
        if(task_id < num_total_tasks_){
            runnable_ ->runTask(task_id, num_total_tasks_);
            tasks_complete_.fetch_add(1);
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    mutex_->lock();
    runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    tasks_complete_ = 0;
    tasks_left_ = num_total_tasks;
    mutex_->unlock();
    while (tasks_complete_ < num_total_tasks){

    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads) : ITaskSystem(num_threads){    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    num_threads_ = num_threads;
    threads_pool_ = new std::thread[num_threads];
    shut_down_ = false;
    runnable_ = nullptr;
    num_sleeping_threads_ = 0;

    for (int i = 0; i < num_threads; i++) {
        threads_pool_[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::runThreadSleep, this);
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    shut_down_ = true;
    while(num_sleeping_threads_ > 0){
        cv_.notify_all();
    }
    for (int i = 0; i < num_threads_; i++) {
        threads_pool_[i].join();
    }
    delete [] threads_pool_;
}

void TaskSystemParallelThreadPoolSleeping::runThreadSleep() {
    while(true) {
        if (shut_down_) break;
        std::unique_lock<std::mutex> lock(mutex_);
        if(IRunnable *runnable = runnable_){
            int task_id = cur_task_++;
            if(cur_task_ >= num_total_tasks_)
                runnable_ = nullptr;
            lock.unlock();
            runnable->runTask(task_id, num_total_tasks_); 
        }else{
            num_sleeping_threads_++;
            cv_.wait(lock);
            num_sleeping_threads_--;
        }
    }
}


void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lock(mutex_);
    runnable_ = runnable;
    num_total_tasks_ = num_total_tasks;
    cur_task_ = 0;
    lock.unlock();
    cv_.notify_all();
    while(runnable_ || num_sleeping_threads_ < num_threads_){

    };
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
