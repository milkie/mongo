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

#include "mongo/pch.h"

#include "mongo/db/repl/rs_thread_pool.h"

#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>

#include "mongo/db/jsobj.h"

namespace mongo {
    namespace replset {
        void ThreadPool::waitForWork() {
            SimpleMutex::scoped_lock lck(_batchMx);
            while (!_running) {
                _batchCV.wait(_batchMx);
            }
        }
        
        void ThreadPool::incrementFinished() {
            SimpleMutex::scoped_lock lck(_batchMx);
            ++_finishedCount;
            if (_finishedCount == _nThreads) {
                _finishCV.notify_one();
            }
            verify(_finishedCount < _nThreads);
        }

        void ThreadPool::setTask(Task func) {
            SimpleMutex::scoped_lock lck(_batchMx);
            verify(!_running);
            _task = func;
        }

        ThreadPool::ThreadPool(int nThreads) : 
            _nThreads(nThreads),
            _batchMx("RS ThreadPool"),
            _finishedCount(0),
            _running(false)
        {
            _workers.reserve(_nThreads);
            for (int i = 0; i < _nThreads; ++i) {
                Worker* worker = new Worker(*this);
                _workers.push_back(worker);
            }        
       
        };

        ThreadPool::~ThreadPool() {
            // unimplemented.
        }

        void ThreadPool::go() {
            SimpleMutex::scoped_lock lck(_batchMx);
            _running = true;
            _batchCV.notify_all();
            while (_finishedCount < _nThreads) {
                _finishCV.wait(_batchMx);
            }
            _running = false;
            _finishedCount = 0;
        }

   

        Worker::Worker(ThreadPool& pool) : 
            _pool(pool),
            _thread(boost::bind(&Worker::loop, this))
        {}
    

        void Worker::enqueue(OpPkg op) {
            _queue.push_back(op);
        }

        void Worker::loop() {
            while (true) {
                _pool.waitForWork();
                while (!_queue.empty()) {
                    OpPkg& op = _queue.front();
                    _pool._task(op);
                    _queue.pop_front();
                }
                _pool.incrementFinished();
            }
        }
    } // namespace replset
} // namespace mongo

