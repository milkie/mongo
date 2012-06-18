// connection_factory.cpp

/**
*    Copyright (C) 2012 10gen Inc.
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

#include "mongo/client/connpool.h"
#include "mongo/db/client.h"
#include "mongo/db/security.h"

// This file contains the server-only (mongod and mongos) implementation of the factory functions
// for getting ScopedDbConnections.  Handles setting authentication info on the underlying
// connection as needed.
namespace mongo {

    ScopedDbConnection* ScopedDbConnection::getScopedDbConnection() {
        ScopedDbConnection* conn = new ScopedDbConnection();
        if ( !noauth ) {
            conn->_conn->setAuthenticationTable(
                    ClientBasic::getCurrent()->getAuthenticationInfo()->getAuthTable() );
        }
        return conn;
    }

    ScopedDbConnection* ScopedDbConnection::getScopedDbConnection(const string& host,
                                                                  double socketTimeout) {
        ScopedDbConnection* conn = new ScopedDbConnection(host, socketTimeout);
        if ( !noauth ) {
            conn->_conn->setAuthenticationTable(
                    ClientBasic::getCurrent()->getAuthenticationInfo()->getAuthTable() );
        }
        return conn;
    }


    ScopedDbConnection* ScopedDbConnection::getInternalScopedDbConnection() {
        ScopedDbConnection* conn = new ScopedDbConnection();
        if ( !noauth ) {
            conn->_conn->setAuthenticationTable(
                    AuthenticationTable::getInternalSecurityAuthenticationTable() );
        }
        return conn;
    }

    ScopedDbConnection* ScopedDbConnection::getInternalScopedDbConnection(const string& host,
                                                                          double socketTimeout) {
        ScopedDbConnection* conn = new ScopedDbConnection(host, socketTimeout);
        if ( !noauth ) {
            conn->_conn->setAuthenticationTable(
                    AuthenticationTable::getInternalSecurityAuthenticationTable() );
        }
        return conn;
    }

}
