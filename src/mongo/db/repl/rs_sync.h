/**
 *    Copyright (C) 2008 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <deque>

#include "mongo/db/oplog.h"
#include "mongo/db/client.h"
#include "mongo/db/repl/rs_thread_pool.h"

namespace mongo {
namespace replset {

    class BackgroundSyncInterface;

    /**
     * "Normal" replica set syncing
     */
    class SyncTail : public Sync {
        BackgroundSyncInterface* _queue;
        typedef void (*multiSyncApplyFunc)(OpPkg op);
    public:
        virtual ~SyncTail();
        SyncTail(BackgroundSyncInterface *q, ThreadPool& writerPool);
        virtual bool syncApply(const BSONObj &o);
        void oplogApplication();
        BSONObj* peek();
        void consume();

        // returns true if we should continue waiting for BSONObjs, false if we should
        // stop waiting and apply the queue we have.  Only returns false if !ops.empty().
        bool tryPopAndWaitForMore(std::deque<const BSONObj*>& ops);
        void clearOps(std::deque<const BSONObj*>& ops);
        bool multiApply(std::deque<const BSONObj*>& ops, multiSyncApplyFunc f);
    private:
        replset::ThreadPool& _writerPool;
        void prefetchOps(std::deque<const BSONObj*>& ops);
        static void prefetchOp(const BSONObj* op);
        void fillWriterQueues(ThreadPool& pool, std::deque<const BSONObj*>& ops);

        void handleSlaveDelay(const BSONObj& op);
    };

    /**
     * Initial clone and sync
     */
    class InitialSync : public SyncTail {
    public:
        virtual ~InitialSync();
        InitialSync(BackgroundSyncInterface *q, ThreadPool& pool);
        bool oplogApplication(const BSONObj& applyGTEObj, const BSONObj& minValidObj);
    };

    // TODO: move hbmsg into an error-keeping class (SERVER-4444)
    void sethbmsg(const string& s, const int logLevel=0);

} // namespace replset
} // namespace mongo
