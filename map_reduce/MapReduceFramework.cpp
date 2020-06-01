#include <pthread.h>
#include <semaphore.h>
#include <vector>
#include <algorithm>
#include <functional>
#include <atomic>
#include <iostream>
#include <set>
#include <queue>
#include <semaphore.h>
#include "Barrier.h"
#include "MapReduceFramework.h"


typedef struct JobContext{
    const int numOfThreads;
    std::vector<IntermediateVec*> *vecOfIntermediateVec;          // pointer to vector of pointers to intermediate vectors of intermediate pairs
    const InputVec input;
    OutputVec &output;
    JobState state;
    const MapReduceClient & client;
    std::queue <IntermediateVec*> * reduceQueue;
    Barrier * barrier;
    std::vector<pthread_t> * threads;                            // vector of threads
    sem_t sem;
    std::atomic<int> atomic_next_task = {0};
    std::atomic<int> finished_map_tasks = {0};
    std::atomic<int> finished_reduce_tasks = {0};
    std::atomic<bool> shffelling = {true};
    int numOfKeys;
    pthread_mutex_t taskMutex;
    pthread_mutex_t outputMutex;
    pthread_mutex_t precentageMutex;

public:
    JobContext(const int numOfThreads, std::vector<IntermediateVec*> * vecOfVecs,
            const InputVec input, OutputVec &output, JobState state, const MapReduceClient &client,
            std::queue<IntermediateVec*> * queue, Barrier * barrier):
        numOfThreads(numOfThreads),
        vecOfIntermediateVec(vecOfVecs),
        input(input),
        output(output),
        state(state),
        client(client),
        reduceQueue(queue),
        barrier(barrier)
    {
        threads = nullptr;
        numOfKeys = 0;

        if(pthread_mutex_init(&taskMutex, nullptr) != EXIT_SUCCESS ||
           pthread_mutex_init(&outputMutex, nullptr) != EXIT_SUCCESS ||
           pthread_mutex_init(&precentageMutex, nullptr) != EXIT_SUCCESS ||
                sem_init(&sem, 0, 0) != 0){
            std::cerr << "error initializing Mutex\n";
            exit(EXIT_FAILURE);
        }
    }
} JobContext;



struct ThreadContext {
    int threadId;
    JobContext * jc;
    Barrier* barrier;
};

struct compForSet {
    bool operator () (const K2 *k1, const K2 *k2) {
        return *k2 < *k1;
    }
};


bool compK2(const IntermediatePair &p1, const IntermediatePair &p2) {
    return (*p1.first) < (*p2.first);
}

int getTaskIndex(ThreadContext * tc){
    if(pthread_mutex_lock(&tc->jc->taskMutex) != 0){            // lock taskMutex
        std::cerr << "error locking taskMutex\n";
        exit(EXIT_FAILURE);
    }
    int taskIndex = tc->jc->atomic_next_task;
    tc->jc->atomic_next_task++;                                 // get next task's index


    if(pthread_mutex_unlock(&tc->jc->taskMutex) != 0){          // lock taskMutex
        std::cerr << "error unlocking taskMutex\n";
        exit(EXIT_FAILURE);
    }
    return taskIndex;
}

void mapPhase(ThreadContext *tc){
    int taskIndex = 0, inputSize = tc->jc->input.size();
    while (true) {
      taskIndex = getTaskIndex(tc);
        if(taskIndex >= inputSize) {
            break;
        }            // stop if there are no more tasks
        else {

            std::pair<K1 *, V1 *> pair = tc->jc->input[taskIndex];        // get pair from input[index]
            tc->jc->client.map(pair.first, pair.second, tc);            // call map to start the task

            if (pthread_mutex_lock(&tc->jc->precentageMutex) != 0) {      // lock precentageMutex
                std::cerr << "error locking precentageMutex\n";
                exit(EXIT_FAILURE);
            }
            tc->jc->finished_map_tasks++;                               // calculate work's precentage
            tc->jc->state.percentage = 100.0 * (float) (tc->jc->finished_map_tasks) / (float) (inputSize);
            if (pthread_mutex_unlock(&tc->jc->precentageMutex) != 0) {    // unlock precentageMutex
                std::cerr << "error unlocking precentageMutex\n";
                exit(EXIT_FAILURE);
            }
        }

    }

    IntermediateVec * vec = (*tc->jc->vecOfIntermediateVec)[tc->threadId];
    std::sort(vec->begin(), vec->end(), compK2);                  // sort
}

void reducePhase(ThreadContext *tc) {
    while (true) {
        if(pthread_mutex_lock(&tc->jc->taskMutex) != 0){                         // lock taskMutex
            std::cerr << "error locking taskMutex\n";
            exit(EXIT_FAILURE);
        }

        if (!tc->jc->shffelling && (*tc->jc->reduceQueue).empty()) {            // if shuffle is over and there are no more vectors to reduce
            if(pthread_mutex_unlock(&tc->jc->taskMutex) != 0){                  // unlock taskMutex
                std::cerr << "error unlocking taskMutex\n";
                exit(EXIT_FAILURE);
            }
            break;                                                              // stop reducePhase
        }
        if ((*tc->jc->reduceQueue).empty()){                                    // if tasks Queue is empty but still shuffeling
            if(pthread_mutex_unlock(&tc->jc->taskMutex) != 0){                  // unlock taskMutex
                std::cerr << "error unlocking taskMutex\n";
                exit(EXIT_FAILURE);
            }
            if (sem_wait(&tc->jc->sem) != 0){                                   // semaphore waits for the shuffle to produce more tasks
                std::cerr << "error in wait func" << " \n";
                exit(EXIT_FAILURE);
            }
            if(pthread_mutex_lock(&tc->jc->taskMutex) != 0){                    // lock taskMutex
                std::cerr << "error locking taskMutex\n";
                exit(EXIT_FAILURE);
            }
        }
        IntermediateVec *vecToReduce = (*tc->jc->reduceQueue).front();          // get the vector (task) form top of the Queue
        (*tc->jc->reduceQueue).pop();                                           // remove the vector prom the queue
        if(pthread_mutex_unlock(&tc->jc->taskMutex) != 0){                      // unlock taskMutex
            std::cerr << "error unlocking taskMutex\n";
            exit(EXIT_FAILURE);
        }

        tc->jc->client.reduce(vecToReduce, tc->jc);                             // call reduce
        vecToReduce->clear();
        delete(vecToReduce);

        if(pthread_mutex_lock(&tc->jc->precentageMutex) != 0){                  // lock precentageMutex
            std::cerr << "error locking precentageMutex\n";
            exit(EXIT_FAILURE);
        }
        tc->jc->finished_reduce_tasks++;                                        // update current percentage
        tc->jc->state.percentage = (float)(100.0 * (tc->jc->finished_reduce_tasks) / (float)(tc->jc->numOfKeys));
        if(pthread_mutex_unlock(&tc->jc->precentageMutex) != 0){                // unlock percentageMutex
            std::cerr << "error unlocking precentageMutex\n";
            exit(EXIT_FAILURE);
        }

    }
}

void shufflePhase(ThreadContext * tc) {
    std::set<K2*, compForSet> keysSet = std::set<K2*, compForSet>();                            // create set of K2* keys
    for(int i = 0; i < tc->jc->numOfThreads; i++) {                                             // for each thread
        int threadVecSize = (int)(*tc->jc->vecOfIntermediateVec)[i]->size();
        for(int j = 0; j < threadVecSize; j++) {                   // for each intermediateVector of it
            K2* key = (*(*tc->jc->vecOfIntermediateVec)[i])[j].first;
            keysSet.insert(key);                                                                // insert its pair's key to the set
        }
    }
    tc->jc->numOfKeys = keysSet.size();

    std::vector<IntermediateVec*> * vector = tc->jc->vecOfIntermediateVec;                      // get the interMediate vector of vectors
    for(auto key = keysSet.begin(); key != keysSet.end(); key++) {
        IntermediateVec * newVec = new IntermediateVec();                                       // create new vector of IntermediatePairs matching this key
        for(int i = 0; i < tc->jc->numOfThreads; i++) {
            while (!(*vector)[i]->empty() && !(*(*vector)[i]->back().first < *(*key))) {        // while there are still pairs in this thread's vector which equal to our key
                newVec->push_back((*vector)[i]->back());                                        // add this pair to the vector created before
                (*vector)[i]->pop_back();                                                       // remove this pair from thread's vector
            }
        }
        if(pthread_mutex_lock(&tc->jc->taskMutex) != 0){                                        // lock taskMutex
            std::cerr << "error locking taskMutex\n";
            exit(EXIT_FAILURE);
        }
        if(! newVec->empty()) {
            tc->jc->reduceQueue->push(newVec);                                                  // add this vector to queue for Reduce
            sem_post(&tc->jc->sem);                                                             // increase semaphore
        }
        if(pthread_mutex_unlock(&tc->jc->taskMutex) != 0){                                      // unlock taskMutex
            std::cerr << "error unlocking taskMutex\n";
            exit(EXIT_FAILURE);
        }

    }

    tc->jc->shffelling = false;                                                                 // end shuffle phase
}

/**
 * this function starts a thread's job - gets the job, calls map and sort the results.
 * @param threadContext ThreadContext object for*tc->jc->vecOfIntermediateVec)[i] the current thread.
 */
void* threadsTasks(void *threadContext){
    ThreadContext * tc = (ThreadContext*) threadContext;

    if(pthread_mutex_lock(&tc->jc->precentageMutex) != 0){
        std::cerr << "error locking taskMutex\n";
        exit(EXIT_FAILURE);
    }
    if (tc->jc->state.stage != MAP_STAGE){
        tc->jc->state.stage = MAP_STAGE;
        tc->jc->state.percentage = 0;
    }
    if(pthread_mutex_unlock(&tc->jc->precentageMutex) != 0){
        std::cerr << "error unlocking taskMutex\n";
        exit(EXIT_FAILURE);
    }

    mapPhase(tc);
    tc->barrier->barrier();

    if(tc->threadId == 0) {
        tc->jc->state.stage = REDUCE_STAGE;
        tc->jc->state.percentage = 0;
        shufflePhase(tc);
    }

    reducePhase(tc);

    //std::reverse(tc->jc->output.begin(), tc->jc->output.end());

    return 0;
}


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext* tc = (ThreadContext*) context;
    IntermediatePair * pair = new IntermediatePair {key, value};
    (*(tc->jc->vecOfIntermediateVec))[tc->threadId]->push_back(*pair);
}

void emit3(K3 *key, V3 *value, void *context) {
    JobContext* jc = (JobContext*) context;
    pthread_mutex_lock(&jc->outputMutex);            // critical code section starts here
    OutputPair * pair = new OutputPair{key, value};
    jc->output.push_back(*pair);
    pthread_mutex_unlock(&jc->outputMutex);            // critical code section starts here

}


JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    std::queue <IntermediateVec*> * queue = new std::queue<IntermediateVec*>();
    std::vector<pthread_t> * threads = new std::vector<pthread_t>(multiThreadLevel);
    std::vector<IntermediateVec*> * vecOfInterVecs = new std::vector<IntermediateVec*>(multiThreadLevel);
    for (int i = 0; i < multiThreadLevel; i++)
        (* vecOfInterVecs)[i] = (IntermediateVec*) new IntermediateVec();

    JobState state = {UNDEFINED_STAGE, 0};

    Barrier * barrier = new Barrier(multiThreadLevel);
    JobContext * jc = new JobContext {multiThreadLevel, vecOfInterVecs, inputVec, outputVec, state, client, queue, barrier};

    for (int i = 0; i < multiThreadLevel; ++i) {
        ThreadContext * tc = (ThreadContext*) new ThreadContext;
        tc->jc = jc;
        tc->threadId = i;
        tc->barrier = barrier;
        
        pthread_create(&(*threads)[i], NULL, threadsTasks, (void*)tc);
    }

    jc->threads = threads;
    return jc;
}


void waitForJob(JobHandle job) {
    JobContext * jc = (JobContext*)job;
   /* while (jc->state.stage != REDUCE_STAGE && jc->state.percentage != 100) {} */

    for(int i = 0; i < jc->numOfThreads; i++) {
        pthread_join((*jc->threads)[i], NULL);
    }

}

void getJobState(JobHandle job, JobState *state) {
    JobContext* jc = (JobContext*)job;
    *state = jc->state;
}

void closeJobHandle(JobHandle job) {
    JobContext* jc = (JobContext*)job;
    waitForJob(job);
    for(int i = 0; i < jc->numOfThreads; i++) {
        (*jc->vecOfIntermediateVec)[i]->clear();
        delete((*jc->vecOfIntermediateVec)[i]);
    }

    delete(jc->vecOfIntermediateVec);
    delete(jc->reduceQueue);
    delete(jc->barrier);
    sem_destroy(&jc->sem);
    pthread_mutex_destroy(&jc->taskMutex);
    pthread_mutex_destroy(&jc->precentageMutex);
    pthread_mutex_destroy(&jc->outputMutex);

    (*jc->threads).clear();
    delete(jc->threads);

    delete(jc);
}

