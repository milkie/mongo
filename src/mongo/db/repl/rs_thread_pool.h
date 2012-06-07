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
    
    struct OpPkg {
        SyncTail* st;
        const BSONObj* op;
    };
    
    class ThreadPool : boost::noncopyable {
    public:
        typedef void (*Task)(OpPkg);

        explicit ThreadPool(int nThreads);

        ~ThreadPool();

        // blocks until all worker queues are exhausted
        void go();

        void waitForEnd();
        void waitForWork();

        std::vector< Worker* > _workers;
        
        void incrementFinished();
        void setTask(Task func);
        Task _task;
        
    private:
        int _nThreads;
        SimpleMutex _batchMx;
        boost::condition_variable_any _batchCV;
        boost::condition_variable_any _finishCV;
        int _finishedCount;
        bool _running;
    };

    class Worker : boost::noncopyable {
    public:
        Worker(ThreadPool& pool);
        ~Worker();
        void enqueue(OpPkg op);
        
    private:
        void loop();

        std::list<OpPkg> _queue;
        ThreadPool& _pool;
        boost::thread _thread;
        }; 
}
}
            

