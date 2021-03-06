// Check that capped collections get an _id index when replicated

// Test #
// 0) create capped collection on replset, check _id index appears on secondary
// 1) create normal collection, then convertToCapped, repeat check from #1

// Create a new replica set test with name 'testSet' and 3 members
var replTest = new ReplSetTest( {name: 'testSet', nodes: 3} );

// call startSet() to start each mongod in the replica set
// this returns a list of nodes
var nodes = replTest.startSet();

// Call initiate() to send the replSetInitiate command
// This will wait for initiation
replTest.initiate();

// Call getMaster to return a reference to the node that's been
// elected master
var master = replTest.getMaster();
// And get the slaves from the liveNodes
var slave1 = replTest.liveNodes.slaves[0];
var slave2 = replTest.liveNodes.slaves[1];

// Calling getMaster made available the liveNodes structure,
// which looks like this:
// liveNodes = {master: masterNode, slaves: [slave1, slave2] }
printjson( replTest.liveNodes );

// define db names to use for this test
var dbname = "dbname";
var masterdb = master.getDB( dbname );
var slave1db = slave1.getDB( dbname );
slave1db.setSlaveOk();
var slave2db = slave2.getDB( dbname );
slave2db.setSlaveOk();

var numtests = 2;
for( testnum=0; testnum < numtests; testnum++ ){

    //define collection name
    coll = "coll" + testnum;

    // drop the coll on the master (just in case it already existed)
    // and wait for the drop to replicate
    masterdb.getCollection( coll ).drop();
    replTest.awaitReplication();

    if ( testnum == 0 ){
        // create a capped collection on the master
        // insert a bunch of things in it
        // wait for it to replicate
        masterdb.runCommand( {create : coll , capped : true , size : 1024} );
        for(i=0; i < 500 ; i++){
            masterdb.getCollection( coll ).insert( {a: 1000} );
        }
        replTest.awaitReplication();
    }
    else if ( testnum == 1 ){
        // create a non-capped collection on the master
        // insert a bunch of things in it
        // wait for it to replicate
        masterdb.runCommand( {create : coll } );
        for(i=0; i < 500 ; i++){
            masterdb.getCollection( coll ).insert( {a: 1000} );
        }
        replTest.awaitReplication();

        // make sure _id index exists on primary
        assert.eq( 1 ,
                   masterdb.system.indexes.find( { key:{"_id" : 1}, ns: dbname + "." + coll } ).count() ,
                   "master does not have _id index on normal collection");

        // then convert it to capped
        masterdb.runCommand({convertToCapped: coll , size: 1024 } );
        replTest.awaitReplication();
    }

    // what indexes do we have?
    print("**********Master indexes:**********");
    masterdb.system.indexes.find().forEach(printjson);
    print("");

    print("**********Slave1 indexes:**********");
    slave1db.system.indexes.find().forEach(printjson);
    print("");

    print("**********Slave2 indexes:**********");
    slave2db.system.indexes.find().forEach(printjson);
    print("");

    // insure each slave has _id index, but not master
    assert.eq( 0 ,
               masterdb.system.indexes.find( { key:{"_id" : 1}, ns: dbname + "." + coll } ).count() ,
               "master has an _id index on capped collection");
    assert.eq( 1 ,
               slave1db.system.indexes.find( { key:{"_id" : 1}, ns: dbname + "." + coll } ).count() ,
               "slave1 does not have _id index on capped collection");
    assert.eq( 1 ,
               slave2db.system.indexes.find( { key:{"_id" : 1}, ns: dbname + "." + coll } ).count() ,
               "slave2 does not have _id index on capped collection");

    print("capped_id.js Test # " + testnum + " SUCCESS");
}

//Finally, stop set
replTest.stopSet();


