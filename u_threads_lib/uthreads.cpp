#include <list>
#include <unordered_set>
#include <vector>
#include <string>
#include <iostream>
#include <sys/time.h>
#include <signal.h>
#include <setjmp.h>
#include "uthreads.h"
#include "sleeping_threads_list.h"
#include <unistd.h>

#ifdef __x86_64__
/* code for 64 bit Intel arch */

typedef unsigned long address_t;
#define JB_SP 6
#define JB_PC 7

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%fs:0x30,%0\n"
                 "rol    $0x11,%0\n"
    : "=g" (ret)
    : "0" (addr));
    return ret;
}

#else
/* code for 32 bit Intel arch */

typedef unsigned int address_t;
#define JB_SP 4
#define JB_PC 5

/* A translation is required when using an address of a variable.
   Use this as a black box in your code. */
address_t translate_address(address_t addr)
{
    address_t ret;
    asm volatile("xor    %%gs:0x18,%0\n"
		"rol    $0x9,%0\n"
                 : "=g" (ret)
                 : "0" (addr));
    return ret;
}

#endif


/* struct representing a single thread */
typedef struct Trd{
    sigjmp_buf env;        // env = environment variable
    char *stack;
    int id;
    bool sleeping = false;     // if the thread is sleeping
    bool blocked = false;
    int calling_times = 0;
}Trd;

/* a queue to where we add the ready threads */
std::list<int> READY;

/* a vector in which we save all existing threads  at all states */
std::vector<Trd*> living_threads (MAX_THREAD_NUM);

/* the index of the current running thread (only one running each time) */
int current_running_thread = 0;

/* time outcast for running for each thread */
int quantum = 0;

/* how many times all threads have run overall */
int total_quantum = 0;

/* struct of virtual signal action - for virtual timer */
struct sigaction vsa = {0};     // virtual

/* struct of real signal action - for realtime timer */
struct sigaction rsa = {0};     // real

/* timer struct */
struct itimerval timer;

/* sleeping threads list as given by course staff */
SleepingThreadsList sleeping_threads;

/* signal set to contain signals we want to block */
sigset_t signals_set;


/*
 * function which calculates the time when a sleeping thread should wake up at
 * @param usecs_to_sleep - hoe long the thread should be sleeping
 * @return the real time when it should wake up
 */
timeval calc_wake_up_timeval(int usecs_to_sleep) {
    timeval now, time_to_sleep, wake_up_timeval;
    gettimeofday(&now, nullptr);
    time_to_sleep.tv_sec = usecs_to_sleep / 1000000;
    time_to_sleep.tv_usec = usecs_to_sleep % 1000000;
    timeradd(&now, &time_to_sleep, &wake_up_timeval);
    return wake_up_timeval;
}

/*
 * deletes allocations before exiting the program.
 */
void free_allocations(){
    for (int i = 0; i< MAX_THREAD_NUM; i++){
        if (living_threads[i]){
            delete living_threads[i]->stack;
            delete living_threads[i];
            living_threads[i] = nullptr;
        }
    }
}


/*
 * updates fields for thread jump and jump to the next thread.
 */
void activate_next_thread(){
    int currentThread = READY.front();
    total_quantum += 1;
    living_threads[currentThread]->calling_times += 1;
    READY.pop_front();
    current_running_thread = currentThread;
    setitimer (ITIMER_VIRTUAL, &timer, NULL);
    siglongjmp(living_threads[currentThread]->env, 1);
}

/*
 * save current thread environment, and calls to activate_next_thread().
 */
void switch_threads()
{
    int current_thread = uthread_get_tid();
    int ret_val = sigsetjmp(living_threads[current_thread]->env, 1);
    if (ret_val != 0) {     // if we return from a long jump (restores)
        // unblocks signals SIGALRM, SIGVTALRM
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return;
    }
    // else - we want to create a new jump - activate the next ready thread available
    activate_next_thread();
}

/*
 * virtual timer handler (handles SIGVTALRM)
 * we're getting here when quantum is over.
 */
void vt_timer_handler(int sig){
    // blocks signals SIGALRM, SIGVTALRM
    sigprocmask(SIG_BLOCK, &signals_set, NULL);

    READY.push_back(uthread_get_tid());
    switch_threads();
}

 /*
  * real timer handler (handle SIGALRM)
  * we're getting here when one of the sleeping signals is waking.
  * @param sig
  */
void real_timer_handler(int sig){
     // blocks signals SIGALRM, SIGVTALRM
     sigprocmask(SIG_BLOCK, &signals_set, NULL);

     timeval now, res, time_left;
     int id;

     while (sleeping_threads.peek() != nullptr) {
        // if thread was terminated and still exists in sleeping_threads_list
         if (living_threads[sleeping_threads.peek()->id] == nullptr) {
             sleeping_threads.pop();
             continue;
         }
         // checks whether we need to wake this thread up
         gettimeofday(&now, nullptr);
         time_left = sleeping_threads.peek()->awaken_tv;
         timersub(&time_left, &now, &res);
         auto us = res.tv_usec + res.tv_sec*1000000;
         if (us <= 0){            // time to wake up!
             id = sleeping_threads.peek()->id;
             living_threads[id]->sleeping = false;
             sleeping_threads.pop();
             if (!living_threads[id]->blocked) {
                 READY.push_back(id);
             }
         }
         else {                    // steel not time to wakeup. set next alarm if there are more sleeping threads
             timeval next = sleeping_threads.peek()->awaken_tv;
             gettimeofday(&now, nullptr);
             timersub(&next, &now, &res);
             ualarm((useconds_t)(res.tv_usec + res.tv_sec*1000000), 0);
             break;
        }
    }
    //unblock signals
    sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
}

/*
 * prints error messages as bad allocation etc.
 */
void print_system_error(const std::string &text){
    std::cerr << "system error: " << text << std::endl;
    free_allocations();
    exit(1);
}

/*
 * prints errors in given params atc.
 */
void print_thread_lib_error(const std::string &text){
    std::cerr << "thread library error: " << text << std::endl;
}


int uthread_init(int quantum_usecs){
    if(quantum_usecs <= 0){
        print_thread_lib_error("invalid quantum");
        return -1;
    }
    quantum = quantum_usecs;
    total_quantum = 1;
    current_running_thread = 0;

    // initializing main thread in threads' list
    try {
        Trd *trd = new Trd();
        trd->id = 0;
        trd->blocked = false;
        trd->sleeping = false;
        trd->calling_times = 1;
        living_threads[0] = trd;
    } catch (const std::bad_alloc& e){
        print_system_error("allocation failed");
    }


    // config vt_timer_handler as the signal handler for SIGVTALRM.
    vsa.sa_handler = &vt_timer_handler;
    vsa.sa_mask = signals_set;
    sigaction(SIGVTALRM, &vsa,NULL);
    // Configure the timer for the first time */
    timer.it_value.tv_sec = 0;		// first time interval, seconds part
    timer.it_value.tv_usec = quantum_usecs;		// first time interval, microseconds part
    // configure the timer to expire every quantum_usec after that.
    timer.it_interval.tv_sec = 0;
    timer.it_interval.tv_usec = quantum_usecs;
    // Start a virtual timer. It counts down whenever this process is executing.
    setitimer (ITIMER_VIRTUAL, &timer, NULL);

    // Install real_timer_handler as the signal handler for SIGALRM.
    rsa.sa_handler = &real_timer_handler;
    rsa.sa_mask = signals_set;
    sigaction(SIGALRM, &rsa,NULL);

    // initialize signals set and adding SIGALRM, SIGVTALRM to it
    sigemptyset(&signals_set);
    sigaddset(&signals_set, SIGALRM);
    sigaddset(&signals_set, SIGVTALRM);

    return 0;
}

/*
 * gets id number, and a pointer to a void function,
 * and creates a thread with this this id that points to this function.
 * if allocation didn't succeed, prints an error messege and exit code.
 */
void create_new_thread(int id, void (*f)()){
    address_t sp, pc;

    try {
        Trd *new_thread = new Trd();
        new_thread->stack = new char[STACK_SIZE];
        new_thread->id = id;
        new_thread->calling_times = 0;
        living_threads[id] = new_thread;

        // handle the thread's attributes
        sp = (address_t)new_thread->stack + STACK_SIZE - sizeof(address_t);
        pc = (address_t)f;
        sigsetjmp(living_threads[id]->env, 1);
        (living_threads[id]->env->__jmpbuf)[JB_SP] = translate_address(sp);
        (living_threads[id]->env->__jmpbuf)[JB_PC] = translate_address(pc);
        sigemptyset(&living_threads[id]->env->__saved_mask);

        READY.push_back(id);
    } catch (const std::bad_alloc& e){
        print_system_error("allocation failed");
    }
}


int uthread_spawn(void (*f)()){
    // blocks signals SIGALRM, SIGVTALRM
    sigprocmask(SIG_BLOCK, &signals_set, NULL);

    int available_id = 0;
    // looks for the min id, and check if creating a new thread won't exceed the max num of threads.
    for(int i = 1; i < MAX_THREAD_NUM; i++){                       // 0 is main thread
        if (!living_threads[i])                                    // if nullptr = available
        {
            available_id = i;
            break;
        }
    }
    if(!available_id){                                              // min id is larger than max threads allowed.
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        print_thread_lib_error("max number of threads reached");
        return -1;
    }
    // else - create new thread and define it as Ready
    create_new_thread(available_id, f);

    //unblock signals
    sigprocmask(SIG_UNBLOCK, &signals_set, NULL);

    return available_id;
}


int uthread_terminate(int tid){
    // blocks signals SIGALRM, SIGVTALRM
    sigprocmask(SIG_BLOCK, &signals_set, NULL);

    if (tid == 0){                                                           // if main thread
        free_allocations();
        exit(0);
    }
    if(tid < 0 || tid >= MAX_THREAD_NUM || !living_threads[tid]){            // nullptr = does't exist
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        print_thread_lib_error("can't terminate unexisting thread");
        return -1;
    }

    else if (tid == uthread_get_tid()) {                                  // running thread
        delete living_threads[tid]->stack;
        delete living_threads[tid];
        living_threads[tid] = nullptr;
        activate_next_thread();
    }
    else if (!living_threads[tid]->blocked && !living_threads[tid]->sleeping){      // in READY list
        READY.remove(tid);
    }
    delete living_threads[tid];
    living_threads[tid] = nullptr;                                            // for sleeping or blocked threads (and ready)

    //unblock signals
    sigprocmask(SIG_UNBLOCK, &signals_set, NULL);

    return 0;
}

int uthread_block(int tid) {
    if (tid == 0)                                                        // main thread
    {
        print_thread_lib_error("main thread cannot be blcoked");
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }
    if (tid < 0 || tid >= MAX_THREAD_NUM || !living_threads[tid]){          //doesn't exist
        print_thread_lib_error("thread doesn't exist");
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }
    if (living_threads[tid]->blocked){                                     // blocked
        print_thread_lib_error("thread already blocked");
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }
    if (tid == uthread_get_tid()){                                        // running thread
        living_threads[tid]->blocked = true;
        switch_threads();        // activate another thread instead
        return 0;
    }
    if (!living_threads[tid]->sleeping){                                  // ready
        READY.remove(tid);
        living_threads[tid]->blocked = true;
    }
    // else - sleeping
    living_threads[tid]->blocked = true;

    //unblock signals
    sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
    return 0;
}

int uthread_resume(int tid){
    // blocks signals SIGALRM, SIGVTALRM
    sigprocmask(SIG_BLOCK, &signals_set, NULL);

    if(tid < 0 || tid >= MAX_THREAD_NUM || !living_threads[tid]){
        print_thread_lib_error("can't resume unexisting thread");
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }

    if (living_threads[tid]->blocked){                             // blocked
        living_threads[tid]->blocked = false;
        if (!living_threads[tid]->sleeping) {                     // blocked and not sleeping
            READY.push_back(tid);
        }
        //unblock signals
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return 0;
    }


    else {                     // not blocked
        print_thread_lib_error("can't resume unblocked thread");
        //unblock signals
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }

}

int uthread_sleep(unsigned int usec){
    // blocks signals SIGALRM, SIGVTALRM
    sigprocmask(SIG_BLOCK, &signals_set, NULL);

    if (current_running_thread == 0){                                // main thread
        print_thread_lib_error("main thread can't be sleeping");
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }

    if (usec == 0){
        READY.push_back(uthread_get_tid());
        switch_threads();
        return 0;
    }

    living_threads[uthread_get_tid()]->sleeping = true;

    // adds the current running thread to the sleeping threads
    timeval time = calc_wake_up_timeval(usec);
    sleeping_threads.add(uthread_get_tid(), time);

    // calculate time to next alarm
    timeval now, wakeup_time, res;
    gettimeofday(&now, nullptr);
    wakeup_time = sleeping_threads.peek()->awaken_tv;
    timersub(&wakeup_time, &now, &res);

    // updates the alarm to be the earliest, meaning top ot sleeping list
    ualarm((useconds_t)(res.tv_usec + res.tv_sec*1000000), 0);
    switch_threads();

    //unblock signals
    sigprocmask(SIG_UNBLOCK, &signals_set, NULL);

    return 0;
}


int uthread_get_tid(){
    int id = current_running_thread;
    return id;
}

int uthread_get_total_quantums(){
    return total_quantum;
}

int uthread_get_quantums(int tid){
    // blocks signals SIGALRM, SIGVTALRM
    sigprocmask(SIG_BLOCK, &signals_set, NULL);

    if (tid >= MAX_THREAD_NUM || tid < 0 || !living_threads[tid]){          // doesn't exist
        print_thread_lib_error("thread doesn't exist");
        sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
        return -1;
    }

    //unblock signals
    sigprocmask(SIG_UNBLOCK, &signals_set, NULL);
    return living_threads[tid]->calling_times;
}




