import test from 'node:test';
import assert from 'node:assert/strict';
import {Connector,ConnectorJournal,ConnectorWatchdog} from '../src/connector.ts';
import type {ConnectorFrame,ConnectorReply,ConnectorTransport} from '../src/connector.ts';

const owner={ownerId:'independent-review',pid:90001,processStart:'synthetic-start',bootId:'synthetic-boot'};
class Link implements ConnectorTransport {
  readonly connectionKey:string;
  connected=0;closed=0;requests:ConnectorFrame[]=[];
  constructor(key='a'.repeat(64)){this.connectionKey=key;}
  async connect(){this.connected++;}
  async close(){this.closed++;}
  async request(frame:ConnectorFrame):Promise<ConnectorReply>{this.requests.push(frame);return {id:frame.id,generation:frame.generation,ok:true,data:{product:'AgentFleet',protocolVersion:1}};}
}

test('watchdog cannot call an opened but unverified transport healthy',async()=>{
  const journal=new ConnectorJournal(':memory:'),link=new Link(),connector=new Connector(journal,link,owner);
  try{
    await connector.ensureConnected();journal.enqueue('queued-task',{version:1,op:'claim'});
    const result=await new ConnectorWatchdog(journal).tick({nowMs:100,owned:connector});
    assert.equal(result.state,'reconciliation-required');assert.equal(link.connected,1);assert.equal(link.requests.length,0);
    assert.equal(journal.status().outbox[0].state,'queued');
  }finally{await connector.close();journal.close();}
});

test('reconciliation cannot smuggle a claim through its read-only transport facade',async()=>{
  const journal=new ConnectorJournal(':memory:'),link=new Link();
  const connector=new Connector(journal,link,owner,{reconcileConnection:async(transport,generation,snapshot)=>{
    await transport.request({id:'smuggled-claim',generation,payload:{version:1,op:'claim'}});
    return {helloConfirmed:true,replayCursor:snapshot.serverCursor};
  }});
  try{
    await connector.ensureConnected();const result=await new ConnectorWatchdog(journal).tick({nowMs:100,owned:connector});
    assert.equal(result.state,'blocked');assert.equal(link.requests.length,0);assert.equal(journal.status().synchronizedGeneration,null);
  }finally{await connector.close();journal.close();}
});

test('a persisted journal cannot carry queued work to a different fixed connection',async()=>{
  const journal=new ConnectorJournal(':memory:'),first=new Link(),one=new Connector(journal,first,owner),second=new Link('b'.repeat(64)),two=new Connector(journal,second,owner);
  try{
    await one.ensureConnected();journal.enqueue('for-original-target',{version:1,op:'claim'});await one.close();
    const result=await two.ensureConnected();assert.equal(result.state,'blocked');assert.equal(second.connected,0);
    assert.equal(journal.status().connectionKey,first.connectionKey);assert.equal(journal.status().outbox[0].state,'queued');
  }finally{await one.close();await two.close();journal.close();}
});

test('a hanging startup handshake is bounded and cannot become healthy',async()=>{
  const journal=new ConnectorJournal(':memory:'),link=new Link();
  const connector=new Connector(journal,link,owner,{operationTimeoutMs:20,reconcileConnection:async()=>new Promise(()=>{})});
  try{
    await connector.ensureConnected();const result=await new ConnectorWatchdog(journal).tick({nowMs:100,owned:connector});
    assert.equal(result.state,'backoff');assert(link.closed>0);assert.equal(journal.status().synchronizedGeneration,null);
  }finally{await connector.close();journal.close();}
});
