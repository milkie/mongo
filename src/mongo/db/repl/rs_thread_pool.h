/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#pragma once

#include <boost/thread/thread.hpp>
#include <boost/thread/condition_variable.hpp>

#include "mongo/util/concurrency/mutex.h"

namespace mongo {

    class BSONObj;

namespace replset {

    class Worker;
    class SyncTail;


    // An oplog op plus the SyncTail object,
    // wrapped up in a nice package so we can give it
    // to each Worker in the pool.
    struct OpPkg {
        SyncTail* st;
        const BSONObj* op;
    };
    

    // This ThreadPool maintains separate queues for each
    // thread, unlike the generic ThreadPool in utils/.
    class ThreadPool : boost::noncopyable {
    public:
        typedef void (*Task)(OpPkg);

        explicit ThreadPool(int nThreads);

        ~ThreadPool();

        // blocks until all worker queues are exhausted
        void go();
        
        // sets the Task function that the Workers use to do work
        void setTask(Task func);

        // enqueues some work for a particular Worker
        void enqueue(int workerNumber, OpPkg& op);

        // Used internally by the Workers for synchronization 
        void waitForEnd();
        void waitForWork();
        void incrementFinished();
        Task task;

    private:
        int _nThreads;

        // This mutex is used for both of the condvars
        SimpleMutex _batchMx;
        boost::condition_variable_any _batchCV;
        boost::condition_variable_any _finishCV;
        int _finishedCount;
        bool _running;

        std::vector< Worker* > _workers;
    };

    class Worker : boost::noncopyable {
    public:
        Worker(ThreadPool& pool);
        ~Worker();
        void enqueue(OpPkg op);
        
    private:
        void loop();

        std::list<OpPkg> _queue;
        // A reference back to the parent
        ThreadPool& _pool;
        boost::thread _thread;
        }; 
}
}
            

