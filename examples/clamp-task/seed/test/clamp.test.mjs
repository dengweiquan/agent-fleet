import test from 'node:test';
import assert from 'node:assert/strict';
import {clamp} from '../src/clamp.js';
test('inclusive bounds and interior',()=>{assert.equal(clamp(-2,0,10),0);assert.equal(clamp(20,0,10),10);assert.equal(clamp(4,0,10),4);assert.equal(clamp(0,0,10),0);assert.equal(clamp(10,0,10),10);assert.equal(clamp(7,3,3),3);});
test('fractional and negative bounds',()=>{assert.equal(clamp(-2.5,-3,-1),-2.5);assert.equal(clamp(1.2,0,1),1);});
test('invalid ranges and nonfinite arguments',()=>{for(const a of [[1,3,2],[NaN,0,1],[Infinity,0,1],[1,-Infinity,2],[1,0,NaN]])assert.throws(()=>clamp(...a),RangeError);});
