var TD=Object.defineProperty,AD=Object.defineProperties;var RD=Object.getOwnPropertyDescriptors;var Sg=Object.getOwnPropertySymbols;var OD=Object.prototype.hasOwnProperty,FD=Object.prototype.propertyIsEnumerable;var Eg=(t,n,e)=>n in t?TD(t,n,{enumerable:!0,configurable:!0,writable:!0,value:e}):t[n]=e,g=(t,n)=>{for(var e in n||={})OD.call(n,e)&&Eg(t,e,n[e]);if(Sg)for(var e of Sg(n))FD.call(n,e)&&Eg(t,e,n[e]);return t},R=(t,n)=>AD(t,RD(n));var wt=null,Gs=!1,Pr=1,PD=null,et=Symbol("SIGNAL");function O(t){let n=wt;return wt=t,n}function Ks(){return wt}var Vr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function ar(t){if(Gs)throw new Error("");if(wt===null)return;wt.consumerOnSignalRead(t);let n=wt.producersTail;if(n!==void 0&&n.producer===t)return;let e,r=wt.recomputing;if(r&&(e=n!==void 0?n.nextProducer:wt.producers,e!==void 0&&e.producer===t)){wt.producersTail=e,e.lastReadVersion=t.version,e.knownValidAtEpoch=Pr;return}let i=t.consumersTail;if(i!==void 0&&i.consumer===wt&&(!r||i.knownValidAtEpoch===Pr))return;let o=Ri(wt),a={producer:t,consumer:wt,nextProducer:e,prevConsumer:void 0,knownValidAtEpoch:Pr,lastReadVersion:t.version,nextConsumer:void 0};wt.producersTail=a,n!==void 0?n.nextProducer=a:wt.producers=a,o&&Ng(t,a)}function kg(){Pr++}function ia(t){if(!(Ri(t)&&!t.dirty)&&!(!t.dirty&&t.lastCleanEpoch===Pr)){if(!t.producerMustRecompute(t)&&!Ai(t)){Ys(t);return}t.producerRecomputeValue(t),Ys(t)}}function Cu(t){if(t.consumers===void 0)return;let n=Gs;Gs=!0;try{for(let e=t.consumers;e!==void 0;e=e.nextConsumer){let r=e.consumer;r.dirty||LD(r)}}finally{Gs=n}}function Du(){return wt?.consumerAllowSignalWrites!==!1}function LD(t){t.dirty=!0,Cu(t),t.consumerMarkedDirty?.(t)}function Ys(t){t.dirty=!1,t.lastCleanEpoch=Pr}function sr(t){return t&&Ig(t),O(t)}function Ig(t){if(t.producersTail?.knownValidAtEpoch===Pr){let n=t.producers;for(;n!==void 0;)n.knownValidAtEpoch=null,n=n.nextProducer}t.producersTail=void 0,t.recomputing=!0}function jr(t,n){O(n),t&&Mg(t)}function Mg(t){t.recomputing=!1;let n=t.producersTail,e=n!==void 0?n.nextProducer:t.producers;if(e!==void 0){if(Ri(t))do e=Su(e);while(e!==void 0);n!==void 0?n.nextProducer=void 0:t.producers=void 0}}function Ai(t){for(let n=t.producers;n!==void 0;n=n.nextProducer){let e=n.producer,r=n.lastReadVersion;if(r!==e.version||(ia(e),r!==e.version))return!0}return!1}function lr(t){if(Ri(t)){let n=t.producers;for(;n!==void 0;)n=Su(n)}t.producers=void 0,t.producersTail=void 0,t.consumers=void 0,t.consumersTail=void 0}function Ng(t,n){let e=t.consumersTail,r=Ri(t);if(e!==void 0?(n.nextConsumer=e.nextConsumer,e.nextConsumer=n):(n.nextConsumer=void 0,t.consumers=n),n.prevConsumer=e,t.consumersTail=n,!r)for(let i=t.producers;i!==void 0;i=i.nextProducer)Ng(i.producer,i)}function Su(t){let n=t.producer,e=t.nextProducer,r=t.nextConsumer,i=t.prevConsumer;if(t.nextConsumer=void 0,t.prevConsumer=void 0,r!==void 0?r.prevConsumer=i:n.consumersTail=i,i!==void 0)i.nextConsumer=r;else if(n.consumers=r,!Ri(n)){let o=n.producers;for(;o!==void 0;)o=Su(o)}return e}function Ri(t){return t.consumerIsAlwaysLive||t.consumers!==void 0}function Zs(t){PD?.(t)}function Qs(t,n){return Object.is(t,n)}function oa(t,n){let e=Object.create(VD);e.computation=t,n!==void 0&&(e.equal=n);let r=()=>{if(ia(e),ar(e),e.value===Lr)throw e.error;return e.value};return r[et]=e,Zs(e),r}var Ws=Symbol("UNSET"),qs=Symbol("COMPUTING"),Lr=Symbol("ERRORED"),VD=R(g({},Vr),{value:Ws,dirty:!0,error:null,equal:Qs,kind:"computed",producerMustRecompute(t){return t.value===Ws||t.value===qs},producerRecomputeValue(t){if(t.value===qs)throw new Error("");let n=t.value;t.value=qs;let e=sr(t),r,i=!1;try{r=t.computation(),O(null),i=n!==Ws&&n!==Lr&&r!==Lr&&t.equal(n,r)}catch(o){r=Lr,t.error=o}finally{jr(t,e)}if(i){t.value=n;return}t.value=r,t.version++}});function jD(){throw new Error}var Tg=jD;function Ag(t){Tg(t)}function Eu(t){Tg=t}var BD=null;function ku(t,n){let e=Object.create(Oi);e.value=t,n!==void 0&&(e.equal=n);let r=()=>Rg(e);return r[et]=e,Zs(e),[r,a=>Br(e,a),a=>Iu(e,a)]}function Rg(t){return ar(t),t.value}function Br(t,n){Du()||Ag(t),t.equal(t.value,n)||(t.value=n,zD(t))}function Iu(t,n){Du()||Ag(t),Br(t,n(t.value))}var Oi=R(g({},Vr),{equal:Qs,value:void 0,kind:"signal"});function zD(t){t.version++,kg(),Cu(t),BD?.(t)}var Mu=R(g({},Vr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function Nu(t){if(t.dirty=!1,t.version>0&&!Ai(t))return;t.version++;let n=sr(t);try{t.cleanup(),t.fn()}finally{jr(t,n)}}var Tu;function Xs(){return Tu}function Cn(t){let n=Tu;return Tu=t,n}var Og=Symbol("NotFound");function Fi(t){return t===Og||t?.name==="\u0275NotFound"}function Fg(t){let n=O(null);try{return t()}finally{O(n)}}function Q(t){return typeof t=="function"}function Pi(t){let e=t(r=>{Error.call(r),r.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Js=Pi(t=>function(e){t(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((r,i)=>`${i+1}) ${r.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function zr(t,n){if(t){let e=t.indexOf(n);0<=e&&t.splice(e,1)}}var ue=class t{constructor(n){this.initialTeardown=n,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let n;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:r}=this;if(Q(r))try{r()}catch(o){n=o instanceof Js?o.errors:[o]}let{_finalizers:i}=this;if(i){this._finalizers=null;for(let o of i)try{Pg(o)}catch(a){n=n??[],a instanceof Js?n=[...n,...a.errors]:n.push(a)}}if(n)throw new Js(n)}}add(n){var e;if(n&&n!==this)if(this.closed)Pg(n);else{if(n instanceof t){if(n.closed||n._hasParent(this))return;n._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(n)}}_hasParent(n){let{_parentage:e}=this;return e===n||Array.isArray(e)&&e.includes(n)}_addParent(n){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(n),e):e?[e,n]:n}_removeParent(n){let{_parentage:e}=this;e===n?this._parentage=null:Array.isArray(e)&&zr(e,n)}remove(n){let{_finalizers:e}=this;e&&zr(e,n),n instanceof t&&n._removeParent(this)}};ue.EMPTY=(()=>{let t=new ue;return t.closed=!0,t})();var Au=ue.EMPTY;function el(t){return t instanceof ue||t&&"closed"in t&&Q(t.remove)&&Q(t.add)&&Q(t.unsubscribe)}function Pg(t){Q(t)?t():t.unsubscribe()}var ln={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Li={setTimeout(t,n,...e){let{delegate:r}=Li;return r?.setTimeout?r.setTimeout(t,n,...e):setTimeout(t,n,...e)},clearTimeout(t){let{delegate:n}=Li;return(n?.clearTimeout||clearTimeout)(t)},delegate:void 0};function tl(t){Li.setTimeout(()=>{let{onUnhandledError:n}=ln;if(n)n(t);else throw t})}function aa(){}var Lg=Ru("C",void 0,void 0);function Vg(t){return Ru("E",void 0,t)}function jg(t){return Ru("N",t,void 0)}function Ru(t,n,e){return{kind:t,value:n,error:e}}var Hr=null;function Vi(t){if(ln.useDeprecatedSynchronousErrorHandling){let n=!Hr;if(n&&(Hr={errorThrown:!1,error:null}),t(),n){let{errorThrown:e,error:r}=Hr;if(Hr=null,e)throw r}}else t()}function Bg(t){ln.useDeprecatedSynchronousErrorHandling&&Hr&&(Hr.errorThrown=!0,Hr.error=t)}var Ur=class extends ue{constructor(n){super(),this.isStopped=!1,n?(this.destination=n,el(n)&&n.add(this)):this.destination=$D}static create(n,e,r){return new zn(n,e,r)}next(n){this.isStopped?Fu(jg(n),this):this._next(n)}error(n){this.isStopped?Fu(Vg(n),this):(this.isStopped=!0,this._error(n))}complete(){this.isStopped?Fu(Lg,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(n){this.destination.next(n)}_error(n){try{this.destination.error(n)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},HD=Function.prototype.bind;function Ou(t,n){return HD.call(t,n)}var Pu=class{constructor(n){this.partialObserver=n}next(n){let{partialObserver:e}=this;if(e.next)try{e.next(n)}catch(r){nl(r)}}error(n){let{partialObserver:e}=this;if(e.error)try{e.error(n)}catch(r){nl(r)}else nl(n)}complete(){let{partialObserver:n}=this;if(n.complete)try{n.complete()}catch(e){nl(e)}}},zn=class extends Ur{constructor(n,e,r){super();let i;if(Q(n)||!n)i={next:n??void 0,error:e??void 0,complete:r??void 0};else{let o;this&&ln.useDeprecatedNextContext?(o=Object.create(n),o.unsubscribe=()=>this.unsubscribe(),i={next:n.next&&Ou(n.next,o),error:n.error&&Ou(n.error,o),complete:n.complete&&Ou(n.complete,o)}):i=n}this.destination=new Pu(i)}};function nl(t){ln.useDeprecatedSynchronousErrorHandling?Bg(t):tl(t)}function UD(t){throw t}function Fu(t,n){let{onStoppedNotification:e}=ln;e&&Li.setTimeout(()=>e(t,n))}var $D={closed:!0,next:aa,error:UD,complete:aa};var ji=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Ot(t){return t}function rl(...t){return Lu(t)}function Lu(t){return t.length===0?Ot:t.length===1?t[0]:function(e){return t.reduce((r,i)=>i(r),e)}}var q=class t{constructor(n){n&&(this._subscribe=n)}lift(n){let e=new t;return e.source=this,e.operator=n,e}subscribe(n,e,r){let i=WD(n)?n:new zn(n,e,r);return Vi(()=>{let{operator:o,source:a}=this;i.add(o?o.call(i,a):a?this._subscribe(i):this._trySubscribe(i))}),i}_trySubscribe(n){try{return this._subscribe(n)}catch(e){n.error(e)}}forEach(n,e){return e=zg(e),new e((r,i)=>{let o=new zn({next:a=>{try{n(a)}catch(s){i(s),o.unsubscribe()}},error:i,complete:r});this.subscribe(o)})}_subscribe(n){var e;return(e=this.source)===null||e===void 0?void 0:e.subscribe(n)}[ji](){return this}pipe(...n){return Lu(n)(this)}toPromise(n){return n=zg(n),new n((e,r)=>{let i;this.subscribe(o=>i=o,o=>r(o),()=>e(i))})}};q.create=t=>new q(t);function zg(t){var n;return(n=t??ln.Promise)!==null&&n!==void 0?n:Promise}function GD(t){return t&&Q(t.next)&&Q(t.error)&&Q(t.complete)}function WD(t){return t&&t instanceof Ur||GD(t)&&el(t)}function qD(t){return Q(t?.lift)}function ie(t){return n=>{if(qD(n))return n.lift(function(e){try{return t(e,this)}catch(r){this.error(r)}});throw new TypeError("Unable to lift unknown Observable type")}}function le(t,n,e,r,i){return new Vu(t,n,e,r,i)}var Vu=class extends Ur{constructor(n,e,r,i,o,a){super(n),this.onFinalize=o,this.shouldUnsubscribe=a,this._next=e?function(s){try{e(s)}catch(l){n.error(l)}}:super._next,this._error=i?function(s){try{i(s)}catch(l){n.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=r?function(){try{r()}catch(s){n.error(s)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var n;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((n=this.onFinalize)===null||n===void 0||n.call(this))}}};var Hg=Pi(t=>function(){t(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var E=class extends q{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(n){let e=new il(this,this);return e.operator=n,e}_throwIfClosed(){if(this.closed)throw new Hg}next(n){Vi(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let e of this.currentObservers)e.next(n)}})}error(n){Vi(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=n;let{observers:e}=this;for(;e.length;)e.shift().error(n)}})}complete(){Vi(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:n}=this;for(;n.length;)n.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var n;return((n=this.observers)===null||n===void 0?void 0:n.length)>0}_trySubscribe(n){return this._throwIfClosed(),super._trySubscribe(n)}_subscribe(n){return this._throwIfClosed(),this._checkFinalizedStatuses(n),this._innerSubscribe(n)}_innerSubscribe(n){let{hasError:e,isStopped:r,observers:i}=this;return e||r?Au:(this.currentObservers=null,i.push(n),new ue(()=>{this.currentObservers=null,zr(i,n)}))}_checkFinalizedStatuses(n){let{hasError:e,thrownError:r,isStopped:i}=this;e?n.error(r):i&&n.complete()}asObservable(){let n=new q;return n.source=this,n}};E.create=(t,n)=>new il(t,n);var il=class extends E{constructor(n,e){super(),this.destination=n,this.source=e}next(n){var e,r;(r=(e=this.destination)===null||e===void 0?void 0:e.next)===null||r===void 0||r.call(e,n)}error(n){var e,r;(r=(e=this.destination)===null||e===void 0?void 0:e.error)===null||r===void 0||r.call(e,n)}complete(){var n,e;(e=(n=this.destination)===null||n===void 0?void 0:n.complete)===null||e===void 0||e.call(n)}_subscribe(n){var e,r;return(r=(e=this.source)===null||e===void 0?void 0:e.subscribe(n))!==null&&r!==void 0?r:Au}};var tt=class extends E{constructor(n){super(),this._value=n}get value(){return this.getValue()}_subscribe(n){let e=super._subscribe(n);return!e.closed&&n.next(this._value),e}getValue(){let{hasError:n,thrownError:e,_value:r}=this;if(n)throw e;return this._throwIfClosed(),r}next(n){super.next(this._value=n)}};var sa={now(){return(sa.delegate||Date).now()},delegate:void 0};var ol=class extends E{constructor(n=1/0,e=1/0,r=sa){super(),this._bufferSize=n,this._windowTime=e,this._timestampProvider=r,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,n),this._windowTime=Math.max(1,e)}next(n){let{isStopped:e,_buffer:r,_infiniteTimeWindow:i,_timestampProvider:o,_windowTime:a}=this;e||(r.push(n),!i&&r.push(o.now()+a)),this._trimBuffer(),super.next(n)}_subscribe(n){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(n),{_infiniteTimeWindow:r,_buffer:i}=this,o=i.slice();for(let a=0;a<o.length&&!n.closed;a+=r?1:2)n.next(o[a]);return this._checkFinalizedStatuses(n),e}_trimBuffer(){let{_bufferSize:n,_timestampProvider:e,_buffer:r,_infiniteTimeWindow:i}=this,o=(i?1:2)*n;if(n<1/0&&o<r.length&&r.splice(0,r.length-o),!i){let a=e.now(),s=0;for(let l=1;l<r.length&&r[l]<=a;l+=2)s=l;s&&r.splice(0,s+1)}}};var al=class extends ue{constructor(n,e){super()}schedule(n,e=0){return this}};var la={setInterval(t,n,...e){let{delegate:r}=la;return r?.setInterval?r.setInterval(t,n,...e):setInterval(t,n,...e)},clearInterval(t){let{delegate:n}=la;return(n?.clearInterval||clearInterval)(t)},delegate:void 0};var sl=class extends al{constructor(n,e){super(n,e),this.scheduler=n,this.work=e,this.pending=!1}schedule(n,e=0){var r;if(this.closed)return this;this.state=n;let i=this.id,o=this.scheduler;return i!=null&&(this.id=this.recycleAsyncId(o,i,e)),this.pending=!0,this.delay=e,this.id=(r=this.id)!==null&&r!==void 0?r:this.requestAsyncId(o,this.id,e),this}requestAsyncId(n,e,r=0){return la.setInterval(n.flush.bind(n,this),r)}recycleAsyncId(n,e,r=0){if(r!=null&&this.delay===r&&this.pending===!1)return e;e!=null&&la.clearInterval(e)}execute(n,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let r=this._execute(n,e);if(r)return r;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(n,e){let r=!1,i;try{this.work(n)}catch(o){r=!0,i=o||new Error("Scheduled action threw falsy error")}if(r)return this.unsubscribe(),i}unsubscribe(){if(!this.closed){let{id:n,scheduler:e}=this,{actions:r}=e;this.work=this.state=this.scheduler=null,this.pending=!1,zr(r,this),n!=null&&(this.id=this.recycleAsyncId(e,n,null)),this.delay=null,super.unsubscribe()}}};var Bi=class t{constructor(n,e=t.now){this.schedulerActionCtor=n,this.now=e}schedule(n,e=0,r){return new this.schedulerActionCtor(this,n).schedule(r,e)}};Bi.now=sa.now;var ll=class extends Bi{constructor(n,e=Bi.now){super(n,e),this.actions=[],this._active=!1}flush(n){let{actions:e}=this;if(this._active){e.push(n);return}let r;this._active=!0;do if(r=n.execute(n.state,n.delay))break;while(n=e.shift());if(this._active=!1,r){for(;n=e.shift();)n.unsubscribe();throw r}}};var ca=new ll(sl),Ug=ca;var qe=new q(t=>t.complete());function cl(t){return t&&Q(t.schedule)}function ju(t){return t[t.length-1]}function dl(t){return Q(ju(t))?t.pop():void 0}function Dn(t){return cl(ju(t))?t.pop():void 0}function $g(t,n){return typeof ju(t)=="number"?t.pop():n}function Wg(t,n,e,r){function i(o){return o instanceof e?o:new e(function(a){a(o)})}return new(e||(e=Promise))(function(o,a){function s(d){try{c(r.next(d))}catch(m){a(m)}}function l(d){try{c(r.throw(d))}catch(m){a(m)}}function c(d){d.done?o(d.value):i(d.value).then(s,l)}c((r=r.apply(t,n||[])).next())})}function Gg(t){var n=typeof Symbol=="function"&&Symbol.iterator,e=n&&t[n],r=0;if(e)return e.call(t);if(t&&typeof t.length=="number")return{next:function(){return t&&r>=t.length&&(t=void 0),{value:t&&t[r++],done:!t}}};throw new TypeError(n?"Object is not iterable.":"Symbol.iterator is not defined.")}function $r(t){return this instanceof $r?(this.v=t,this):new $r(t)}function qg(t,n,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var r=e.apply(t,n||[]),i,o=[];return i=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),s("next"),s("throw"),s("return",a),i[Symbol.asyncIterator]=function(){return this},i;function a(f){return function(h){return Promise.resolve(h).then(f,m)}}function s(f,h){r[f]&&(i[f]=function(w){return new Promise(function(I,P){o.push([f,w,I,P])>1||l(f,w)})},h&&(i[f]=h(i[f])))}function l(f,h){try{c(r[f](h))}catch(w){p(o[0][3],w)}}function c(f){f.value instanceof $r?Promise.resolve(f.value.v).then(d,m):p(o[0][2],f)}function d(f){l("next",f)}function m(f){l("throw",f)}function p(f,h){f(h),o.shift(),o.length&&l(o[0][0],o[0][1])}}function Yg(t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var n=t[Symbol.asyncIterator],e;return n?n.call(t):(t=typeof Gg=="function"?Gg(t):t[Symbol.iterator](),e={},r("next"),r("throw"),r("return"),e[Symbol.asyncIterator]=function(){return this},e);function r(o){e[o]=t[o]&&function(a){return new Promise(function(s,l){a=t[o](a),i(s,l,a.done,a.value)})}}function i(o,a,s,l){Promise.resolve(l).then(function(c){o({value:c,done:s})},a)}}var ul=(t=>t&&typeof t.length=="number"&&typeof t!="function");function ml(t){return Q(t?.then)}function fl(t){return Q(t[ji])}function pl(t){return Symbol.asyncIterator&&Q(t?.[Symbol.asyncIterator])}function hl(t){return new TypeError(`You provided ${t!==null&&typeof t=="object"?"an invalid object":`'${t}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function YD(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var gl=YD();function bl(t){return Q(t?.[gl])}function vl(t){return qg(this,arguments,function*(){let e=t.getReader();try{for(;;){let{value:r,done:i}=yield $r(e.read());if(i)return yield $r(void 0);yield yield $r(r)}}finally{e.releaseLock()}})}function yl(t){return Q(t?.getReader)}function Se(t){if(t instanceof q)return t;if(t!=null){if(fl(t))return KD(t);if(ul(t))return ZD(t);if(ml(t))return QD(t);if(pl(t))return Kg(t);if(bl(t))return XD(t);if(yl(t))return JD(t)}throw hl(t)}function KD(t){return new q(n=>{let e=t[ji]();if(Q(e.subscribe))return e.subscribe(n);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function ZD(t){return new q(n=>{for(let e=0;e<t.length&&!n.closed;e++)n.next(t[e]);n.complete()})}function QD(t){return new q(n=>{t.then(e=>{n.closed||(n.next(e),n.complete())},e=>n.error(e)).then(null,tl)})}function XD(t){return new q(n=>{for(let e of t)if(n.next(e),n.closed)return;n.complete()})}function Kg(t){return new q(n=>{e0(t,n).catch(e=>n.error(e))})}function JD(t){return Kg(vl(t))}function e0(t,n){var e,r,i,o;return Wg(this,void 0,void 0,function*(){try{for(e=Yg(t);r=yield e.next(),!r.done;){let a=r.value;if(n.next(a),n.closed)return}}catch(a){i={error:a}}finally{try{r&&!r.done&&(o=e.return)&&(yield o.call(e))}finally{if(i)throw i.error}}n.complete()})}function kt(t,n,e,r=0,i=!1){let o=n.schedule(function(){e(),i?t.add(this.schedule(null,r)):this.unsubscribe()},r);if(t.add(o),!i)return o}function _l(t,n=0){return ie((e,r)=>{e.subscribe(le(r,i=>kt(r,t,()=>r.next(i),n),()=>kt(r,t,()=>r.complete(),n),i=>kt(r,t,()=>r.error(i),n)))})}function xl(t,n=0){return ie((e,r)=>{r.add(t.schedule(()=>e.subscribe(r),n))})}function Zg(t,n){return Se(t).pipe(xl(n),_l(n))}function Qg(t,n){return Se(t).pipe(xl(n),_l(n))}function Xg(t,n){return new q(e=>{let r=0;return n.schedule(function(){r===t.length?e.complete():(e.next(t[r++]),e.closed||this.schedule())})})}function Jg(t,n){return new q(e=>{let r;return kt(e,n,()=>{r=t[gl](),kt(e,n,()=>{let i,o;try{({value:i,done:o}=r.next())}catch(a){e.error(a);return}o?e.complete():e.next(i)},0,!0)}),()=>Q(r?.return)&&r.return()})}function wl(t,n){if(!t)throw new Error("Iterable cannot be null");return new q(e=>{kt(e,n,()=>{let r=t[Symbol.asyncIterator]();kt(e,n,()=>{r.next().then(i=>{i.done?e.complete():e.next(i.value)})},0,!0)})})}function eb(t,n){return wl(vl(t),n)}function tb(t,n){if(t!=null){if(fl(t))return Zg(t,n);if(ul(t))return Xg(t,n);if(ml(t))return Qg(t,n);if(pl(t))return wl(t,n);if(bl(t))return Jg(t,n);if(yl(t))return eb(t,n)}throw hl(t)}function Pe(t,n){return n?tb(t,n):Se(t)}function Y(...t){let n=Dn(t);return Pe(t,n)}function Bu(t,n){let e=Q(t)?t:()=>t,r=i=>i.error(e());return new q(n?i=>n.schedule(r,0,i):r)}function da(t){return!!t&&(t instanceof q||Q(t.lift)&&Q(t.subscribe))}var Gr=Pi(t=>function(){t(this),this.name="EmptyError",this.message="no elements in sequence"});function nb(t){return t instanceof Date&&!isNaN(t)}function _e(t,n){return ie((e,r)=>{let i=0;e.subscribe(le(r,o=>{r.next(t.call(n,o,i++))}))})}var{isArray:t0}=Array;function n0(t,n){return t0(n)?t(...n):t(n)}function Cl(t){return _e(n=>n0(t,n))}var{isArray:r0}=Array,{getPrototypeOf:i0,prototype:o0,keys:a0}=Object;function Dl(t){if(t.length===1){let n=t[0];if(r0(n))return{args:n,keys:null};if(s0(n)){let e=a0(n);return{args:e.map(r=>n[r]),keys:e}}}return{args:t,keys:null}}function s0(t){return t&&typeof t=="object"&&i0(t)===o0}function Sl(t,n){return t.reduce((e,r,i)=>(e[r]=n[i],e),{})}function ua(...t){let n=Dn(t),e=dl(t),{args:r,keys:i}=Dl(t);if(r.length===0)return Pe([],n);let o=new q(l0(r,n,i?a=>Sl(i,a):Ot));return e?o.pipe(Cl(e)):o}function l0(t,n,e=Ot){return r=>{rb(n,()=>{let{length:i}=t,o=new Array(i),a=i,s=i;for(let l=0;l<i;l++)rb(n,()=>{let c=Pe(t[l],n),d=!1;c.subscribe(le(r,m=>{o[l]=m,d||(d=!0,s--),s||r.next(e(o.slice()))},()=>{--a||r.complete()}))},r)},r)}}function rb(t,n,e){t?kt(e,t,n):n()}function ib(t,n,e,r,i,o,a,s){let l=[],c=0,d=0,m=!1,p=()=>{m&&!l.length&&!c&&n.complete()},f=w=>c<r?h(w):l.push(w),h=w=>{o&&n.next(w),c++;let I=!1;Se(e(w,d++)).subscribe(le(n,P=>{i?.(P),o?f(P):n.next(P)},()=>{I=!0},void 0,()=>{if(I)try{for(c--;l.length&&c<r;){let P=l.shift();a?kt(n,a,()=>h(P)):h(P)}p()}catch(P){n.error(P)}}))};return t.subscribe(le(n,f,()=>{m=!0,p()})),()=>{s?.()}}function Ct(t,n,e=1/0){return Q(n)?Ct((r,i)=>_e((o,a)=>n(r,o,i,a))(Se(t(r,i))),e):(typeof n=="number"&&(e=n),ie((r,i)=>ib(r,i,t,e)))}function El(t=1/0){return Ct(Ot,t)}function ob(){return El(1)}function cr(...t){return ob()(Pe(t,Dn(t)))}function ma(t){return new q(n=>{Se(t()).subscribe(n)})}function zu(...t){let n=dl(t),{args:e,keys:r}=Dl(t),i=new q(o=>{let{length:a}=e;if(!a){o.complete();return}let s=new Array(a),l=a,c=a;for(let d=0;d<a;d++){let m=!1;Se(e[d]).subscribe(le(o,p=>{m||(m=!0,c--),s[d]=p},()=>l--,void 0,()=>{(!l||!m)&&(c||o.next(r?Sl(r,s):s),o.complete())}))}});return n?i.pipe(Cl(n)):i}function ab(t=0,n,e=Ug){let r=-1;return n!=null&&(cl(n)?e=n:r=n),new q(i=>{let o=nb(t)?+t-e.now():t;o<0&&(o=0);let a=0;return e.schedule(function(){i.closed||(i.next(a++),0<=r?this.schedule(void 0,r):i.complete())},o)})}function Wr(...t){let n=Dn(t),e=$g(t,1/0),r=t;return r.length?r.length===1?Se(r[0]):El(e)(Pe(r,n)):qe}function Ye(t,n){return ie((e,r)=>{let i=0;e.subscribe(le(r,o=>t.call(n,o,i++)&&r.next(o)))})}function sb(t){return ie((n,e)=>{let r=!1,i=null,o=null,a=!1,s=()=>{if(o?.unsubscribe(),o=null,r){r=!1;let c=i;i=null,e.next(c)}a&&e.complete()},l=()=>{o=null,a&&e.complete()};n.subscribe(le(e,c=>{r=!0,i=c,o||Se(t(c)).subscribe(o=le(e,s,l))},()=>{a=!0,(!r||!o||o.closed)&&e.complete()}))})}function kl(t,n=ca){return sb(()=>ab(t,n))}function fa(t){return ie((n,e)=>{let r=null,i=!1,o;r=n.subscribe(le(e,void 0,void 0,a=>{o=Se(t(a,fa(t)(n))),r?(r.unsubscribe(),r=null,o.subscribe(e)):i=!0})),i&&(r.unsubscribe(),r=null,o.subscribe(e))})}function Il(t,n){return Q(n)?Ct(t,n,1):Ct(t,1)}function Hu(t,n=ca){return ie((e,r)=>{let i=null,o=null,a=null,s=()=>{if(i){i.unsubscribe(),i=null;let c=o;o=null,r.next(c)}};function l(){let c=a+t,d=n.now();if(d<c){i=this.schedule(void 0,c-d),r.add(i);return}s()}e.subscribe(le(r,c=>{o=c,a=n.now(),i||(i=n.schedule(l,t),r.add(i))},()=>{s(),r.complete()},void 0,()=>{o=i=null}))})}function lb(t){return ie((n,e)=>{let r=!1;n.subscribe(le(e,i=>{r=!0,e.next(i)},()=>{r||e.next(t),e.complete()}))})}function Ft(t){return t<=0?()=>qe:ie((n,e)=>{let r=0;n.subscribe(le(e,i=>{++r<=t&&(e.next(i),t<=r&&e.complete())}))})}function Ml(t,n=Ot){return t=t??c0,ie((e,r)=>{let i,o=!0;e.subscribe(le(r,a=>{let s=n(a);(o||!t(i,s))&&(o=!1,i=s,r.next(a))}))})}function c0(t,n){return t===n}function cb(t=d0){return ie((n,e)=>{let r=!1;n.subscribe(le(e,i=>{r=!0,e.next(i)},()=>r?e.complete():e.error(t())))})}function d0(){return new Gr}function Uu(t){return ie((n,e)=>{try{n.subscribe(e)}finally{e.add(t)}})}function Hn(t,n){let e=arguments.length>=2;return r=>r.pipe(t?Ye((i,o)=>t(i,o,r)):Ot,Ft(1),e?lb(n):cb(()=>new Gr))}function Nl(t){return t<=0?()=>qe:ie((n,e)=>{let r=[];n.subscribe(le(e,i=>{r.push(i),t<r.length&&r.shift()},()=>{for(let i of r)e.next(i);e.complete()},void 0,()=>{r=null}))})}function Tl(){return ie((t,n)=>{let e,r=!1;t.subscribe(le(n,i=>{let o=e;e=i,r&&n.next([o,i]),r=!0}))})}function db(t={}){let{connector:n=()=>new E,resetOnError:e=!0,resetOnComplete:r=!0,resetOnRefCountZero:i=!0}=t;return o=>{let a,s,l,c=0,d=!1,m=!1,p=()=>{s?.unsubscribe(),s=void 0},f=()=>{p(),a=l=void 0,d=m=!1},h=()=>{let w=a;f(),w?.unsubscribe()};return ie((w,I)=>{c++,!m&&!d&&p();let P=l=l??n();I.add(()=>{c--,c===0&&!m&&!d&&(s=$u(h,i))}),P.subscribe(I),!a&&c>0&&(a=new zn({next:$=>P.next($),error:$=>{m=!0,p(),s=$u(f,e,$),P.error($)},complete:()=>{d=!0,p(),s=$u(f,r),P.complete()}}),Se(w).subscribe(a))})(o)}}function $u(t,n,...e){if(n===!0){t();return}if(n===!1)return;let r=new zn({next:()=>{r.unsubscribe(),t()}});return Se(n(...e)).subscribe(r)}function Al(t,n,e){let r,i=!1;return t&&typeof t=="object"?{bufferSize:r=1/0,windowTime:n=1/0,refCount:i=!1,scheduler:e}=t:r=t??1/0,db({connector:()=>new ol(r,n,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:i})}function pa(t){return Ye((n,e)=>t<=e)}function Ht(...t){let n=Dn(t);return ie((e,r)=>{(n?cr(t,e,n):cr(t,e)).subscribe(r)})}function Pt(t,n){return ie((e,r)=>{let i=null,o=0,a=!1,s=()=>a&&!i&&r.complete();e.subscribe(le(r,l=>{i?.unsubscribe();let c=0,d=o++;Se(t(l,d)).subscribe(i=le(r,m=>r.next(n?n(l,m,d,c++):m),()=>{i=null,s()}))},()=>{a=!0,s()}))})}function Ue(t){return ie((n,e)=>{Se(t).subscribe(le(e,()=>e.complete(),aa)),!e.closed&&n.subscribe(e)})}function Ut(t,n,e){let r=Q(t)||n||e?{next:t,error:n,complete:e}:t;return r?ie((i,o)=>{var a;(a=r.subscribe)===null||a===void 0||a.call(r);let s=!0;i.subscribe(le(o,l=>{var c;(c=r.next)===null||c===void 0||c.call(r,l),o.next(l)},()=>{var l;s=!1,(l=r.complete)===null||l===void 0||l.call(r),o.complete()},l=>{var c;s=!1,(c=r.error)===null||c===void 0||c.call(r,l),o.error(l)},()=>{var l,c;s&&((l=r.unsubscribe)===null||l===void 0||l.call(r)),(c=r.finalize)===null||c===void 0||c.call(r)}))}):Ot}var D=class extends Error{code;constructor(n,e){super(Qr(n,e)),this.code=n}};function u0(t){return`NG0${Math.abs(t)}`}function Qr(t,n){return`${u0(t)}${n?": "+n:""}`}function ge(t){for(let n in t)if(t[n]===ge)return n;throw Error("")}function hb(t,n){for(let e in n)Object.hasOwn(n,e)&&!Object.hasOwn(t,e)&&(t[e]=n[e])}function _a(t){if(typeof t=="string")return t;if(Array.isArray(t))return`[${t.map(_a).join(", ")}]`;if(t==null)return""+t;let n=t.overriddenName||t.name;if(n)return`${n}`;let e=t.toString();if(e==null)return""+e;let r=e.indexOf(`
`);return r>=0?e.slice(0,r):e}function jl(t,n){return t?n?`${t} ${n}`:t:n||""}var m0=ge({__forward_ref__:ge});function $e(t){return t.__forward_ref__=$e,t}function ot(t){return im(t)?t():t}function im(t){return typeof t=="function"&&Object.hasOwn(t,m0)&&t.__forward_ref__===$e}function me(t){return{token:t.token,providedIn:t.providedIn||null,factory:t.factory,value:void 0}}function re(t){return{providers:t.providers||[],imports:t.imports||[]}}function xa(t){return f0(t,Bl)}function om(t){return xa(t)!==null}function f0(t,n){return Object.hasOwn(t,n)&&t[n]||null}function p0(t){let n=t?.[Bl]??null;return n||null}function Wu(t){return t&&Object.hasOwn(t,Ol)?t[Ol]:null}var Bl=ge({\u0275prov:ge}),Ol=ge({\u0275inj:ge}),b=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(n,e){this._desc=n,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=me({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function am(t){return t&&!!t.\u0275providers}var wa=ge({\u0275cmp:ge}),Ca=ge({\u0275dir:ge}),sm=ge({\u0275pipe:ge}),lm=ge({\u0275mod:ge}),ga=ge({\u0275fac:ge}),Xr=ge({__NG_ELEMENT_ID__:ge}),ub=ge({__NG_ENV_ID__:ge});function gb(t){return Hl(t,"@NgModule"),t[lm]||null}function ur(t){return Hl(t,"@Component"),t[wa]||null}function zl(t){return Hl(t,"@Directive"),t[Ca]||null}function bb(t){return Hl(t,"@Pipe"),t[sm]||null}function Hl(t,n){if(t==null)throw new D(-919,!1)}function Da(t){return typeof t=="string"?t:t==null?"":String(t)}var vb=ge({ngErrorCode:ge}),h0=ge({ngErrorMessage:ge}),g0=ge({ngTokenPath:ge});function cm(t,n){return yb("",-200,n)}function Ul(t,n){throw new D(-201,!1)}function yb(t,n,e){let r=new D(n,t);return r[vb]=n,r[h0]=t,e&&(r[g0]=e),r}function b0(t){return t[vb]}var qu;function _b(){return qu}function Dt(t){let n=qu;return qu=t,n}function dm(t,n,e){let r=xa(t);if(r&&r.providedIn=="root")return r.value===void 0?r.value=r.factory():r.value;if(e&8)return null;if(n!==void 0)return n;Ul(t,"")}var Jr=globalThis;var v0={},qr=v0,y0="__NG_DI_FLAG__",Yu=class{injector;constructor(n){this.injector=n}retrieve(n,e){let r=Yr(e)||0;try{return this.injector.get(n,r&8?null:qr,r)}catch(i){if(Fi(i))return i;throw i}}};function _0(t,n=0){let e=Xs();if(e===void 0)throw new D(-203,!1);if(e===null)return dm(t,void 0,n);{let r=x0(n),i=e.retrieve(t,r);if(Fi(i)){if(r.optional)return null;throw i}return i}}function X(t,n=0){return(_b()||_0)(ot(t),n)}function u(t,n){return X(t,Yr(n))}function Yr(t){return typeof t>"u"||typeof t=="number"?t:0|(t.optional&&8)|(t.host&&1)|(t.self&&2)|(t.skipSelf&&4)}function x0(t){return{optional:!!(t&8),host:!!(t&1),self:!!(t&2),skipSelf:!!(t&4)}}function Ku(t){let n=[];for(let e=0;e<t.length;e++){let r=ot(t[e]);if(Array.isArray(r)){if(r.length===0)throw new D(900,!1);let i,o=0;for(let a=0;a<r.length;a++){let s=r[a],l=w0(s);typeof l=="number"?l===-1?i=s.token:o|=l:i=s}n.push(X(i,o))}else n.push(X(r))}return n}function w0(t){return t[y0]}function dr(t,n){let e=Object.hasOwn(t,ga);return e?t[ga]:null}function xb(t,n,e){if(t.length!==n.length)return!1;for(let r=0;r<t.length;r++){let i=t[r],o=n[r];if(e&&(i=e(i),o=e(o)),o!==i)return!1}return!0}function wb(t){return t.flat(Number.POSITIVE_INFINITY)}function $l(t,n){t.forEach(e=>Array.isArray(e)?$l(e,n):n(e))}function um(t,n,e){n>=t.length?t.push(e):t.splice(n,0,e)}function Sa(t,n){return n>=t.length-1?t.pop():t.splice(n,1)[0]}function Cb(t,n){let e=[];for(let r=0;r<t;r++)e.push(n);return e}function Db(t,n,e,r){let i=t.length;if(i==n)t.push(e,r);else if(i===1)t.push(r,t[0]),t[0]=e;else{for(i--,t.push(t[i-1],t[i]);i>n;){let o=i-2;t[i]=t[o],i--}t[n]=e,t[n+1]=r}}function Gl(t,n,e){let r=Ui(t,n);return r>=0?t[r|1]=e:(r=~r,Db(t,r,n,e)),r}function Wl(t,n){let e=Ui(t,n);if(e>=0)return t[e|1]}function Ui(t,n){return C0(t,n,1)}function C0(t,n,e){let r=0,i=t.length>>e;for(;i!==r;){let o=r+(i-r>>1),a=t[o<<e];if(n===a)return o<<e;a>n?i=o:r=o+1}return~(i<<e)}var mr={},ht=[],$i=new b(""),ql=new b("",-1),mm=new b(""),Hi=class{get(n,e=qr){if(e===qr){let i=yb("",-201);throw i.name="\u0275NotFound",i}return e}};function fr(t){return{\u0275providers:t}}function Sb(t){return fr([{provide:$i,multi:!0,useValue:t}])}function Eb(...t){return{\u0275providers:fm(!0,t),\u0275fromNgModule:!0}}function fm(t,...n){let e=[],r=new Set,i,o=a=>{e.push(a)};return $l(n,a=>{let s=a;Fl(s,o,[],r)&&(i||=[],i.push(s))}),i!==void 0&&kb(i,o),e}function kb(t,n){for(let e=0;e<t.length;e++){let{ngModule:r,providers:i}=t[e];pm(i,o=>{n(o,r)})}}function Fl(t,n,e,r){if(t=ot(t),!t)return!1;let i=null,o=Wu(t),a=!o&&ur(t);if(!o&&!a){let l=t.ngModule;if(o=Wu(l),o)i=l;else return!1}else{if(a&&!a.standalone)return!1;i=t}let s=r.has(i);if(a){if(s)return!1;if(r.add(i),a.dependencies){let l=typeof a.dependencies=="function"?a.dependencies():a.dependencies;for(let c of l)Fl(c,n,e,r)}}else if(o){if(o.imports!=null&&!s){r.add(i);let c;$l(o.imports,d=>{Fl(d,n,e,r)&&(c||=[],c.push(d))}),c!==void 0&&kb(c,n)}if(!s){let c=dr(i)||(()=>new i);n({provide:i,useFactory:c,deps:ht},i),n({provide:mm,useValue:i,multi:!0},i),n({provide:$i,useValue:()=>X(i),multi:!0},i)}let l=o.providers;if(l!=null&&!s){let c=t;pm(l,d=>{n(d,c)})}}else return!1;return i!==t&&t.providers!==void 0}function pm(t,n){for(let e of t)am(e)&&(e=e.\u0275providers),Array.isArray(e)?pm(e,n):n(e)}var D0=ge({provide:String,useValue:ge});function Ib(t){return t!==null&&typeof t=="object"&&D0 in t}function S0(t){return!!(t&&t.useExisting)}function E0(t){return!!(t&&t.useFactory)}function Kr(t){return typeof t=="function"}function Mb(t){return!!t.useClass}var Ea=new b(""),Rl={},mb={},Gu;function Gi(){return Gu===void 0&&(Gu=new Hi),Gu}var Le=class{},Zr=class extends Le{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(n,e,r,i){super(),this.parent=e,this.source=r,this.scopes=i,Qu(n,a=>this.processProvider(a)),this.records.set(ql,zi(void 0,this)),i.has("environment")&&this.records.set(Le,zi(void 0,this));let o=this.records.get(Ea);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(mm,ht,{self:!0}))}retrieve(n,e){let r=Yr(e)||0;try{return this.get(n,qr,r)}catch(i){if(Fi(i))return i;throw i}}destroy(){ha(this),this._destroyed=!0;let n=O(null);try{for(let r of this._ngOnDestroyHooks)r.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let r of e)r()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),O(n)}}onDestroy(n){return ha(this),this._onDestroyHooks.push(n),()=>this.removeOnDestroy(n)}runInContext(n){ha(this);let e=Cn(this),r=Dt(void 0),i;try{return n()}finally{Cn(e),Dt(r)}}get(n,e=qr,r){if(ha(this),Object.hasOwn(n,ub))return n[ub](this);let i=Yr(r),o,a=Cn(this),s=Dt(void 0);try{if(!(i&4)){let c=this.records.get(n);if(c===void 0){let d=T0(n)&&xa(n);d&&this.injectableDefInScope(d)?c=zi(Zu(n),Rl):c=null,this.records.set(n,c)}if(c!=null)return this.hydrate(n,c,i)}let l=i&2?Gi():this.parent;return e=i&8&&e===qr?null:e,l.get(n,e)}catch(l){let c=b0(l);throw c===-200||c===-201?new D(c,null):l}finally{Dt(s),Cn(a)}}resolveInjectorInitializers(){let n=O(null),e=Cn(this),r=Dt(void 0),i;try{let o=this.get($i,ht,{self:!0});for(let a of o)a()}finally{Cn(e),Dt(r),O(n)}}toString(){return"R3Injector[...]"}processProvider(n){n=ot(n);let e=Kr(n)?n:ot(n&&n.provide),r=I0(n);if(!Kr(n)&&n.multi===!0){let i=this.records.get(e);i||(i=zi(void 0,Rl,!0),i.factory=()=>Ku(i.multi),this.records.set(e,i)),e=n,i.multi.push(n)}this.records.set(e,r)}hydrate(n,e,r){let i=O(null);try{if(e.value===mb)throw cm("");return e.value===Rl&&(e.value=mb,e.value=e.factory(void 0,r)),typeof e.value=="object"&&e.value&&N0(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{O(i)}}injectableDefInScope(n){if(!n.providedIn)return!1;let e=ot(n.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(n){let e=this._onDestroyHooks.indexOf(n);e!==-1&&this._onDestroyHooks.splice(e,1)}};function Zu(t){let n=xa(t),e=n!==null?n.factory:dr(t);if(e!==null)return e;if(t instanceof b)throw new D(-204,!1);if(t instanceof Function)return k0(t);throw new D(-204,!1)}function k0(t){if(t.length>0)throw new D(-204,!1);let e=p0(t);return e!==null?()=>e.factory(t):()=>new t}function I0(t){if(Ib(t))return zi(void 0,t.useValue);{let n=hm(t);return zi(n,Rl)}}function hm(t,n,e){let r;if(Kr(t)){let i=ot(t);return dr(i)||Zu(i)}else if(Ib(t))r=()=>ot(t.useValue);else if(E0(t))r=()=>t.useFactory(...Ku(t.deps||[]));else if(S0(t))r=(i,o)=>X(ot(t.useExisting),o!==void 0&&o&8?8:void 0);else{let i=ot(t&&(t.useClass||t.provide));if(M0(t))r=()=>new i(...Ku(t.deps));else return dr(i)||Zu(i)}return r}function ha(t){if(t.destroyed)throw new D(-205,!1)}function zi(t,n,e=!1){return{factory:t,value:n,multi:e?[]:void 0}}function M0(t){return!!t.deps}function N0(t){return t!==null&&typeof t=="object"&&typeof t.ngOnDestroy=="function"}function T0(t){return typeof t=="function"||typeof t=="object"&&t.ngMetadataName==="InjectionToken"}function Qu(t,n){for(let e of t)Array.isArray(e)?Qu(e,n):e&&am(e)?Qu(e.\u0275providers,n):n(e)}function st(t,n){let e;t instanceof Zr?(ha(t),e=t):e=new Yu(t);let r,i=Cn(e),o=Dt(void 0);try{return n()}finally{Cn(i),Dt(o)}}function Nb(){return _b()!==void 0||Xs()!=null}var cn=0,L=1,H=2,Ke=3,$t=4,lt=5,ei=6,Wi=7,Ge=8,qn=9,En=10,Ee=11,qi=12,gm=13,pr=14,gt=15,hr=16,ti=17,kn=18,ka=19,bm=20,Un=21,Yl=22,$n=23,Lt=24,ni=25,Tb=26,Yi=27,ze=28,Ab=1,vm=6,ri=7,Ia=8,ii=9,We=10;function Yn(t){return Array.isArray(t)&&typeof t[Ab]=="object"}function Gt(t){return Array.isArray(t)&&t[Ab]===!0}function ym(t){return(t.flags&4)!==0}function Kn(t){return t.componentOffset>-1}function Ma(t){return(t.flags&1)===1}function In(t){return!!t.template}function Ki(t){return(t[H]&512)!==0}function oi(t){return(t[H]&256)===256}var _m="svg",Rb="math";function ct(t){for(;Array.isArray(t);)t=t[cn];return t}function xm(t,n){return ct(n[t])}function dn(t,n){return ct(n[t.index])}function Kl(t,n){return t.data[n]}function wm(t,n){return t[n]}function Cm(t,n,e,r){e>=t.data.length&&(t.data[e]=null,t.blueprint[e]=null),n[e]=r}function Wt(t,n){let e=n[t];return Yn(e)?e:e[cn]}function Ob(t){return(t[H]&4)===4}function Zl(t){return(t[H]&128)===128}function Fb(t){return Gt(t[Ke])}function qt(t,n){return n==null?null:t[n]}function Dm(t){t[ti]=0}function Sm(t){t[H]&1024||(t[H]|=1024,Zl(t)&&ai(t))}function Pb(t,n){for(;t>0;)n=n[pr],t--;return n}function Na(t){return!!(t[H]&9216||t[Lt]?.dirty)}function Ql(t){t[En].changeDetectionScheduler?.notify(8),t[H]&64&&(t[H]|=1024),Na(t)&&ai(t)}function ai(t){t[En].changeDetectionScheduler?.notify(0);let n=Gn(t);for(;n!==null&&!(n[H]&8192||(n[H]|=8192,!Zl(n)));)n=Gn(n)}function Xl(t,n){if(oi(t))throw new D(911,!1);t[Un]===null&&(t[Un]=[]),t[Un].push(n)}function Lb(t,n){if(t[Un]===null)return;let e=t[Un].indexOf(n);e!==-1&&t[Un].splice(e,1)}function Gn(t){let n=t[Ke];return Gt(n)?n[Ke]:n}function Em(t){return t[Wi]??=[]}function km(t){return t.cleanup??=[]}function Vb(t,n,e,r){let i=Em(n);i.push(e),t.firstCreatePass&&km(t).push(r,i.length-1)}var ne={lFrame:Qb(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Xu=!1;function jb(){return ne.lFrame.elementDepthCount}function Bb(){ne.lFrame.elementDepthCount++}function Im(){ne.lFrame.elementDepthCount--}function Mm(){return ne.bindingsEnabled}function Nm(){return ne.skipHydrationRootTNode!==null}function Tm(t){return ne.skipHydrationRootTNode===t}function Am(){ne.skipHydrationRootTNode=null}function W(){return ne.lFrame.lView}function Ve(){return ne.lFrame.tView}function dt(t){return ne.lFrame.contextLView=t,t[Ge]}function ut(t){return ne.lFrame.contextLView=null,t}function rt(){let t=Rm();for(;t!==null&&t.type===64;)t=t.parent;return t}function Rm(){return ne.lFrame.currentTNode}function zb(){let t=ne.lFrame,n=t.currentTNode;return t.isParent?n:n.parent}function Zi(t,n){let e=ne.lFrame;e.currentTNode=t,e.isParent=n}function Om(){return ne.lFrame.isParent}function Fm(){ne.lFrame.isParent=!1}function Hb(){return ne.lFrame.contextLView}function Pm(){return Xu}function ba(t){let n=Xu;return Xu=t,n}function Ub(){let t=ne.lFrame,n=t.bindingRootIndex;return n===-1&&(n=t.bindingRootIndex=t.tView.bindingStartIndex),n}function $b(){return ne.lFrame.bindingIndex}function Gb(t){return ne.lFrame.bindingIndex=t}function gr(){return ne.lFrame.bindingIndex++}function Jl(t){let n=ne.lFrame,e=n.bindingIndex;return n.bindingIndex=n.bindingIndex+t,e}function Wb(){return ne.lFrame.inI18n}function qb(t,n){let e=ne.lFrame;e.bindingIndex=e.bindingRootIndex=t,ec(n)}function Yb(){return ne.lFrame.currentDirectiveIndex}function ec(t){ne.lFrame.currentDirectiveIndex=t}function Kb(t){let n=ne.lFrame.currentDirectiveIndex;return n===-1?null:t[n]}function tc(){return ne.lFrame.currentQueryIndex}function Ta(t){ne.lFrame.currentQueryIndex=t}function A0(t){let n=t[L];return n.type===2?n.declTNode:n.type===1?t[lt]:null}function Lm(t,n,e){if(e&4){let i=n,o=t;for(;i=i.parent,i===null&&!(e&1);)if(i=A0(o),i===null||(o=o[pr],i.type&10))break;if(i===null)return!1;n=i,t=o}let r=ne.lFrame=Zb();return r.currentTNode=n,r.lView=t,!0}function nc(t){let n=Zb(),e=t[L];ne.lFrame=n,n.currentTNode=e.firstChild,n.lView=t,n.tView=e,n.contextLView=t,n.bindingIndex=e.bindingStartIndex,n.inI18n=!1}function Zb(){let t=ne.lFrame,n=t===null?null:t.child;return n===null?Qb(t):n}function Qb(t){let n={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:t,child:null,inI18n:!1};return t!==null&&(t.child=n),n}function Xb(){let t=ne.lFrame;return ne.lFrame=t.parent,t.currentTNode=null,t.lView=null,t}var Vm=Xb;function rc(){let t=Xb();t.isParent=!0,t.tView=null,t.selectedIndex=-1,t.contextLView=null,t.elementDepthCount=0,t.currentDirectiveIndex=-1,t.currentNamespace=null,t.bindingRootIndex=-1,t.bindingIndex=-1,t.currentQueryIndex=0}function Jb(t){return(ne.lFrame.contextLView=Pb(t,ne.lFrame.contextLView))[Ge]}function Mn(){return ne.lFrame.selectedIndex}function br(t){ne.lFrame.selectedIndex=t}function Aa(){let t=ne.lFrame;return Kl(t.tView,t.selectedIndex)}function un(){ne.lFrame.currentNamespace=_m}function si(){R0()}function R0(){ne.lFrame.currentNamespace=null}function jm(){return ne.lFrame.currentNamespace}var ev=!0;function ic(){return ev}function oc(t){ev=t}function Ju(t,n=null,e=null,r){let i=Bm(t,n,e,r);return i.resolveInjectorInitializers(),i}function Bm(t,n=null,e=null,r,i=new Set){let o=[e||ht,Eb(t)],a;return new Zr(o,n||Gi(),a||null,i)}var J=class t{static THROW_IF_NOT_FOUND=qr;static NULL=new Hi;static create(n,e){if(Array.isArray(n))return Ju({name:""},e,n,"");{let r=n.name??"";return Ju({name:r},n.parent,n.providers,r)}}static \u0275prov=me({token:t,providedIn:"any",factory:()=>X(ql)});static __NG_ELEMENT_ID__=-1},G=new b(""),at=class{static __NG_ELEMENT_ID__=O0;static __NG_ENV_ID__=n=>n},Pl=class extends at{_lView;constructor(n){super(),this._lView=n}get destroyed(){return oi(this._lView)}onDestroy(n){let e=this._lView;return Xl(e,n),()=>Lb(e,n)}};function O0(){return new Pl(W())}var tv=!1,nv=new b(""),vr=(()=>{class t{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new tt(!1);debugTaskTracker=u(nv,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new q(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})(),em=class extends E{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(n=!1){super(),this.__isAsync=n,Nb()&&(this.destroyRef=u(at,{optional:!0})??void 0,this.pendingTasks=u(vr,{optional:!0})??void 0)}emit(n){let e=O(null);try{super.next(n)}finally{O(e)}}subscribe(n,e,r){let i=n,o=e||(()=>null),a=r;if(n&&typeof n=="object"){let l=n;i=l.next?.bind(l),o=l.error?.bind(l),a=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),i&&(i=this.wrapInTimeout(i)),a&&(a=this.wrapInTimeout(a)));let s=super.subscribe({next:i,error:o,complete:a});return n instanceof ue&&n.add(s),s}wrapInTimeout(n){return e=>{let r=this.pendingTasks?.add();setTimeout(()=>{try{n(e)}finally{r!==void 0&&this.pendingTasks?.remove(r)}})}}},C=em;function Ll(...t){}function zm(t){let n,e;function r(){t=Ll;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),n!==void 0&&clearTimeout(n)}catch{}}return n=setTimeout(()=>{t(),r()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{t(),r()})),()=>r()}function rv(t){return queueMicrotask(()=>t()),()=>{t=Ll}}var Hm="isAngularZone",va=Hm+"_ID",F0=0,T=class t{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new C(!1);onMicrotaskEmpty=new C(!1);onStable=new C(!1);onError=new C(!1);constructor(n){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:r=!1,shouldCoalesceRunChangeDetection:i=!1,scheduleInRootZone:o=tv}=n;if(typeof Zone>"u")throw new D(908,!1);Zone.assertZonePatched();let a=this;a._nesting=0,a._outer=a._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(a._inner=a._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(a._inner=a._inner.fork(Zone.longStackTraceZoneSpec)),a.shouldCoalesceEventChangeDetection=!i&&r,a.shouldCoalesceRunChangeDetection=i,a.callbackScheduled=!1,a.scheduleInRootZone=o,V0(a)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Hm)===!0}static assertInAngularZone(){if(!t.isInAngularZone())throw new D(909,!1)}static assertNotInAngularZone(){if(t.isInAngularZone())throw new D(909,!1)}run(n,e,r){return this._inner.run(n,e,r)}runTask(n,e,r,i){let o=this._inner,a=o.scheduleEventTask("NgZoneEvent: "+i,n,P0,Ll,Ll);try{return o.runTask(a,e,r)}finally{o.cancelTask(a)}}runGuarded(n,e,r){return this._inner.runGuarded(n,e,r)}runOutsideAngular(n){return this._outer.run(n)}},P0={};function Um(t){if(t._nesting==0&&!t.hasPendingMicrotasks&&!t.isStable)try{t._nesting++,t.onMicrotaskEmpty.emit(null)}finally{if(t._nesting--,!t.hasPendingMicrotasks)try{t.runOutsideAngular(()=>t.onStable.emit(null))}finally{t.isStable=!0}}}function L0(t){if(t.isCheckStableRunning||t.callbackScheduled)return;t.callbackScheduled=!0;function n(){zm(()=>{t.callbackScheduled=!1,tm(t),t.isCheckStableRunning=!0,Um(t),t.isCheckStableRunning=!1})}t.scheduleInRootZone?Zone.root.run(()=>{n()}):t._outer.run(()=>{n()}),tm(t)}function V0(t){let n=()=>{L0(t)},e=F0++;t._inner=t._inner.fork({name:"angular",properties:{[Hm]:!0,[va]:e,[va+e]:!0},onInvokeTask:(r,i,o,a,s,l)=>{if(j0(l))return r.invokeTask(o,a,s,l);try{return fb(t),r.invokeTask(o,a,s,l)}finally{(t.shouldCoalesceEventChangeDetection&&a.type==="eventTask"||t.shouldCoalesceRunChangeDetection)&&n(),pb(t)}},onInvoke:(r,i,o,a,s,l,c)=>{try{return fb(t),r.invoke(o,a,s,l,c)}finally{t.shouldCoalesceRunChangeDetection&&!t.callbackScheduled&&!B0(l)&&n(),pb(t)}},onHasTask:(r,i,o,a)=>{r.hasTask(o,a),i===o&&(a.change=="microTask"?(t._hasPendingMicrotasks=a.microTask,tm(t),Um(t)):a.change=="macroTask"&&(t.hasPendingMacrotasks=a.macroTask))},onHandleError:(r,i,o,a)=>(r.handleError(o,a),t.runOutsideAngular(()=>t.onError.emit(a)),!1)})}function tm(t){t._hasPendingMicrotasks||(t.shouldCoalesceEventChangeDetection||t.shouldCoalesceRunChangeDetection)&&t.callbackScheduled===!0?t.hasPendingMicrotasks=!0:t.hasPendingMicrotasks=!1}function fb(t){t._nesting++,t.isStable&&(t.isStable=!1,t.onUnstable.emit(null))}function pb(t){t._nesting--,Um(t)}var ya=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new C;onMicrotaskEmpty=new C;onStable=new C;onError=new C;run(n,e,r){return n.apply(e,r)}runGuarded(n,e,r){return n.apply(e,r)}runOutsideAngular(n){return n()}runTask(n,e,r,i){return n.apply(e,r)}};function j0(t){return iv(t,"__ignore_ng_zone__")}function B0(t){return iv(t,"__scheduler_tick__")}function iv(t,n){return!Array.isArray(t)||t.length!==1?!1:t[0]?.data?.[n]===!0}var Sn=class{_console=console;handleError(n){this._console.error("ERROR",n)}};function ov(t){return z0(t)?t:new nm(t)}function z0(t){return t instanceof Error||typeof t=="object"&&t!==null&&typeof t.name=="string"&&typeof t.message=="string"}var nm=class extends Error{constructor(n){super(String(n),{cause:n}),this.name="ErrorBoundaryWrappedError"}},mn=new b("",{factory:()=>{let t=u(T),n=u(Le),e;return r=>{t.runOutsideAngular(()=>{n.destroyed&&!e?setTimeout(()=>{throw r}):(e??=n.get(Sn),e.handleError(r))})}}}),av={provide:$i,useValue:()=>{let t=u(Sn,{optional:!0})},multi:!0},H0=new b("",{factory:()=>{let t=u(G).defaultView;if(!t)return;let n=u(mn),e=o=>{n(o.reason),o.preventDefault()},r=o=>{o.error?n(o.error):n(new Error(o.message,{cause:o})),o.preventDefault()},i=()=>{t.addEventListener("unhandledrejection",e),t.addEventListener("error",r)};typeof Zone<"u"?Zone.root.run(i):i(),u(at).onDestroy(()=>{t.removeEventListener("error",r),t.removeEventListener("unhandledrejection",e)})}});function $m(){return fr([Sb(()=>{u(H0)})])}function A(t,n){let[e,r,i]=ku(t,n?.equal),o=e,a=o[et];return o.set=r,o.update=i,o.asReadonly=sv.bind(o),o}function sv(){let t=this[et];if(t.readonlyFn===void 0){let n=()=>this();n[et]=t,t.readonlyFn=n}return t.readonlyFn}var Zn=new b("",{factory:()=>U0}),U0="ng";var ac=new b(""),li=new b("",{providedIn:"platform",factory:()=>"unknown"}),Ra=new b(""),ci=new b("",{factory:()=>u(G).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Qi=(()=>{class t{view;node;constructor(e,r){this.view=e,this.node=r}static __NG_ELEMENT_ID__=$0}return t})();function $0(){return new Qi(W(),rt())}var Wn=class{},Oa=new b("",{factory:()=>!0});var Gm=new b(""),sc=(()=>{class t{static \u0275prov=me({token:t,providedIn:"root",factory:()=>new rm})}return t})(),rm=class{dirtyEffectCount=0;queues=new Map;add(n){this.enqueue(n),this.schedule(n)}schedule(n){n.dirty&&this.dirtyEffectCount++}remove(n){let e=n.zone,r=this.queues.get(e);r.has(n)&&(r.delete(n),n.dirty&&this.dirtyEffectCount--)}enqueue(n){let e=n.zone;this.queues.has(e)||this.queues.set(e,new Set);let r=this.queues.get(e);r.has(n)||r.add(n)}flush(){for(;this.dirtyEffectCount>0;){let n=!1;for(let[e,r]of this.queues)e===null?n||=this.flushQueue(r):n||=e.run(()=>this.flushQueue(r));n||(this.dirtyEffectCount=0)}}flushQueue(n){let e=!1;for(let r of n)r.dirty&&(this.dirtyEffectCount--,e=!0,r.run());return e}},Vl=class{[et];constructor(n){this[et]=n}destroy(){this[et].destroy()}};function Nn(t,n){let e=n?.injector??u(J),r=n?.manualCleanup!==!0?e.get(at):null,i,o=e.get(Qi,null,{optional:!0}),a=e.get(Wn);return o!==null?(i=cv(o.view,a,t),r instanceof Pl&&r._lView===o.view&&(r=null)):i=q0(t,e.get(sc),a),i.injector=e,r!==null&&(i.onDestroyFns=[r.onDestroy(()=>i.destroy())]),new Vl(i)}var lv=R(g({},Mu),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let t=ba(!1);try{Nu(this)}finally{ba(t)}},cleanup(){if(!this.cleanupFns?.length)return;let t=O(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],O(t)}}}),G0=R(g({},lv),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(lr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.scheduler.remove(this)}}),W0=R(g({},lv),{consumerMarkedDirty(){this.view[H]|=8192,ai(this.view),this.notifier.notify(13)},destroy(){if(lr(this),this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();this.cleanup(),this.view[$n]?.delete(this)}});function cv(t,n,e){let r=Object.create(W0);return r.view=t,r.zone=typeof Zone<"u"?Zone.current:null,r.notifier=n,r.userFn=e,r.fn=dv(r,e),t[$n]??=new Set,t[$n].add(r),r.consumerMarkedDirty(r),r}function q0(t,n,e){let r=Object.create(G0);return r.userFn=t,r.fn=dv(r,t),r.scheduler=n,r.notifier=e,r.zone=typeof Zone<"u"?Zone.current:null,r.scheduler.add(r),r.notifier.notify(12),r}function dv(t,n){return()=>{n(e=>(t.cleanupFns??=[]).push(e))}}function Tn(t){return typeof t=="function"&&t[et]!==void 0}function lc(t){return Tn(t)&&typeof t.set=="function"}var Of=Symbol("InputSignalNode#UNSET"),Hv=R(g({},Oi),{transformFn:void 0,applyValueToInputSignal(t,n){Br(t,n)}});function Wa(t){return{toString:t}.toString()}var pe=(function(t){return t[t.TemplateCreateStart=0]="TemplateCreateStart",t[t.TemplateCreateEnd=1]="TemplateCreateEnd",t[t.TemplateUpdateStart=2]="TemplateUpdateStart",t[t.TemplateUpdateEnd=3]="TemplateUpdateEnd",t[t.LifecycleHookStart=4]="LifecycleHookStart",t[t.LifecycleHookEnd=5]="LifecycleHookEnd",t[t.OutputStart=6]="OutputStart",t[t.OutputEnd=7]="OutputEnd",t[t.BootstrapApplicationStart=8]="BootstrapApplicationStart",t[t.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",t[t.BootstrapComponentStart=10]="BootstrapComponentStart",t[t.BootstrapComponentEnd=11]="BootstrapComponentEnd",t[t.ChangeDetectionStart=12]="ChangeDetectionStart",t[t.ChangeDetectionEnd=13]="ChangeDetectionEnd",t[t.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",t[t.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",t[t.AfterRenderHooksStart=16]="AfterRenderHooksStart",t[t.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",t[t.ComponentStart=18]="ComponentStart",t[t.ComponentEnd=19]="ComponentEnd",t[t.DeferBlockStateStart=20]="DeferBlockStateStart",t[t.DeferBlockStateEnd=21]="DeferBlockStateEnd",t[t.DynamicComponentStart=22]="DynamicComponentStart",t[t.DynamicComponentEnd=23]="DynamicComponentEnd",t[t.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",t[t.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",t})(pe||{}),hc=class{previousValue;currentValue;firstChange;constructor(n,e,r){this.previousValue=n,this.currentValue=e,this.firstChange=r}isFirstChange(){return this.firstChange}};function Uv(t,n,e,r){n!==null?n.applyValueToInputSignal(n,r):t[e]=r}var $v=null,Ae=(()=>{$v=uv;let t=()=>uv;return t.ngInherit=!0,t})();function aS(){return $v}function uv(t){return t.type.prototype.ngOnChanges&&(t.setInput=lS),sS}function sS(){let t=Gv(this),n=t?.current;if(n){let e=t.previous;if(e===mr)t.previous=n;else for(let r in n)e[r]=n[r];t.current=null,this.ngOnChanges(n)}}function lS(t,n,e,r,i){let o=this.declaredInputs[r],a=Gv(t)||cS(t,{previous:mr,current:null}),s=a.current||(a.current={}),l=a.previous,c=l[o];s[o]=new hc(c&&c.currentValue,e,l===mr),Uv(t,n,i,e)}var rf="__ngSimpleChanges__";function Gv(t){return Object.hasOwn(t,rf)&&t[rf]||null}function cS(t,n){return t[rf]=n}var mv=[];var be=function(t,n=null,e){for(let r=0;r<mv.length;r++){let i=mv[r];i(t,n,e)}};function dS(t,n,e){let{ngOnChanges:r,ngOnInit:i,ngDoCheck:o}=n.type.prototype;if(r){let a=aS()(n);(e.preOrderHooks??=[]).push(t,a),(e.preOrderCheckHooks??=[]).push(t,a)}i&&(e.preOrderHooks??=[]).push(0-t,i),o&&((e.preOrderHooks??=[]).push(t,o),(e.preOrderCheckHooks??=[]).push(t,o))}function Wv(t,n){for(let e=n.directiveStart,r=n.directiveEnd;e<r;e++){let o=t.data[e].type.prototype,{ngAfterContentInit:a,ngAfterContentChecked:s,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:d}=o;a&&(t.contentHooks??=[]).push(-e,a),s&&((t.contentHooks??=[]).push(e,s),(t.contentCheckHooks??=[]).push(e,s)),l&&(t.viewHooks??=[]).push(-e,l),c&&((t.viewHooks??=[]).push(e,c),(t.viewCheckHooks??=[]).push(e,c)),d!=null&&(t.destroyHooks??=[]).push(e,d)}}function dc(t,n,e){qv(t,n,3,e)}function uc(t,n,e,r){(t[H]&3)===e&&qv(t,n,e,r)}function Wm(t,n){let e=t[H];(e&3)===n&&(e&=16383,e+=1,t[H]=e)}function qv(t,n,e,r){let i=r!==void 0?t[ti]&65535:0,o=r??-1,a=n.length-1,s=0;for(let l=i;l<a;l++)if(typeof n[l+1]=="number"){if(s=n[l],r!=null&&s>=r)break}else n[l]<0&&(t[ti]+=65536),(s<o||o==-1)&&(uS(t,e,n,l),t[ti]=(t[ti]&4294901760)+l+2),l++}function fv(t,n){be(pe.LifecycleHookStart,t,n);let e=O(null);try{n.call(t)}finally{O(e),be(pe.LifecycleHookEnd,t,n)}}function uS(t,n,e,r){let i=e[r]<0,o=e[r+1],a=i?-e[r]:e[r],s=t[a];i?t[H]>>14<t[ti]>>16&&(t[H]&3)===n&&(t[H]+=16384,fv(s,o)):fv(s,o)}var eo=-1,ui=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(n,e,r,i){this.factory=n,this.name=i,this.canSeeViewProviders=e,this.injectImpl=r}};function mS(t){return(t.flags&8)!==0}function fS(t){return(t.flags&16)!==0}function pS(t,n,e){let r=0;for(;r<e.length;){let i=e[r];if(typeof i=="number"){if(i!==0)break;r++;let o=e[r++],a=e[r++],s=e[r++];t.setAttribute(n,a,s,o)}else{let o=i,a=e[++r];hS(o)?t.setProperty(n,o,a):t.setAttribute(n,o,a),r++}}return r}function Yv(t){return t===3||t===4||t===6}function hS(t){return t.charCodeAt(0)===64}function to(t,n){if(!(n===null||n.length===0))if(t===null||t.length===0)t=n.slice();else{let e=-1;for(let r=0;r<n.length;r++){let i=n[r];typeof i=="number"?e=i:e===0||(e===-1||e===2?pv(t,e,i,null,n[++r]):pv(t,e,i,null,null))}}return t}function pv(t,n,e,r,i){let o=0,a=t.length;if(n===-1)a=-1;else for(;o<t.length;){let s=t[o++];if(typeof s=="number"){if(s===n){a=-1;break}else if(s>n){a=o-1;break}}}for(;o<t.length;){let s=t[o];if(typeof s=="number")break;if(s===e){i!==null&&(t[o+1]=i);return}o++,i!==null&&o++}a!==-1&&(t.splice(a,0,n),o=a+1),t.splice(o++,0,e),i!==null&&t.splice(o++,0,i)}function Kv(t){return t!==eo}function gc(t){return t&32767}function gS(t){return t>>16}function bc(t,n){let e=gS(t),r=n;for(;e>0;)r=r[pr],e--;return r}var of=!0;function vc(t){let n=of;return of=t,n}var bS=256,Zv=bS-1,Qv=5,vS=0,An={};function yS(t,n,e){let r;typeof e=="string"?r=e.charCodeAt(0)||0:Object.hasOwn(e,Xr)&&(r=e[Xr]),r==null&&(r=e[Xr]=vS++);let i=r&Zv,o=1<<i;n.data[t+(i>>Qv)]|=o}function yc(t,n){let e=Xv(t,n);if(e!==-1)return e;let r=n[L];r.firstCreatePass&&(t.injectorIndex=n.length,qm(r.data,t),qm(n,null),qm(r.blueprint,null));let i=Ff(t,n),o=t.injectorIndex;if(Kv(i)){let a=gc(i),s=bc(i,n),l=s[L].data;for(let c=0;c<8;c++)n[o+c]=s[a+c]|l[a+c]}return n[o+8]=i,o}function qm(t,n){t.push(0,0,0,0,0,0,0,0,n)}function Xv(t,n){return t.injectorIndex===-1||t.parent&&t.parent.injectorIndex===t.injectorIndex||n[t.injectorIndex+8]===null?-1:t.injectorIndex}function Ff(t,n){if(t.parent&&t.parent.injectorIndex!==-1)return t.parent.injectorIndex;let e=0,r=null,i=n;for(;i!==null;){if(r=ry(i),r===null)return eo;if(e++,i=i[pr],r.injectorIndex!==-1)return r.injectorIndex|e<<16}return eo}function af(t,n,e){yS(t,n,e)}function _S(t,n){if(n==="class")return t.classes;if(n==="style")return t.styles;let e=t.attrs;if(e){let r=e.length,i=0;for(;i<r;){let o=e[i];if(Yv(o))break;if(o===0)i=i+2;else if(typeof o=="number")for(i++;i<r&&typeof e[i]=="string";)i++;else{if(o===n)return e[i+1];i=i+2}}}return null}function Jv(t,n,e){if(e&8||t!==void 0)return t;Ul(n,"NodeInjector")}function ey(t,n,e,r){if(e&8&&r===void 0&&(r=null),(e&3)===0){let i=t[qn],o=Dt(void 0);try{return i?i.get(n,r,e&8):dm(n,r,e&8)}finally{Dt(o)}}return Jv(r,n,e)}function ty(t,n,e,r=0,i){if(t!==null){if(n[H]&2048&&!(r&2)){let a=DS(t,n,e,r,An);if(a!==An)return a}let o=ny(t,n,e,r,An);if(o!==An)return o}return ey(n,e,r,i)}function ny(t,n,e,r,i){let o=wS(e);if(typeof o=="function"){if(!Lm(n,t,r))return r&1?Jv(i,e,r):ey(n,e,r,i);try{let a;if(a=o(r),a==null&&!(r&8))Ul(e);else return a}finally{Vm()}}else if(typeof o=="number"){let a=null,s=Xv(t,n),l=eo,c=r&1?n[gt][lt]:null;for((s===-1||r&4)&&(l=s===-1?Ff(t,n):n[s+8],l===eo||!gv(r,!1)?s=-1:(a=n[L],s=gc(l),n=bc(l,n)));s!==-1;){let d=n[L];if(hv(o,s,d.data)){let m=xS(s,n,e,a,r,c);if(m!==An)return m}l=n[s+8],l!==eo&&gv(r,n[L].data[s+8]===c)&&hv(o,s,n)?(a=d,s=gc(l),n=bc(l,n)):s=-1}}return i}function xS(t,n,e,r,i,o){let a=n[L],s=a.data[t+8],l=r==null?Kn(s)&&of:r!=a&&(s.type&3)!==0,c=i&1&&o===s,d=mc(s,a,e,l,c);return d!==null?Va(n,a,d,s,i):An}function mc(t,n,e,r,i){let o=t.providerIndexes,a=n.data,s=o&1048575,l=t.directiveStart,c=t.directiveEnd,d=o>>20,m=r?s:s+d,p=i?s+d:c;for(let f=m;f<p;f++){let h=a[f];if(f<l&&e===h||f>=l&&h.type===e)return f}if(i){let f=a[l];if(f&&In(f)&&f.type===e)return l}return null}function Va(t,n,e,r,i){let o=t[e],a=n.data;if(o instanceof ui){let s=o;if(s.resolving)throw cm("");let l=vc(s.canSeeViewProviders);s.resolving=!0;let c=a[e].type||a[e],d,m=s.injectImpl?Dt(s.injectImpl):null,p=Lm(t,r,0);try{o=t[e]=s.factory(void 0,i,a,t,r),n.firstCreatePass&&e>=r.directiveStart&&dS(e,a[e],n)}finally{m!==null&&Dt(m),vc(l),s.resolving=!1,Vm()}}return o}function wS(t){if(typeof t=="string")return t.charCodeAt(0)||0;let n=Object.hasOwn(t,Xr)?t[Xr]:void 0;return typeof n=="number"?n>=0?n&Zv:CS:n}function hv(t,n,e){let r=1<<t;return!!(e[n+(t>>Qv)]&r)}function gv(t,n){return!(t&2)&&!(t&1&&n)}var Qn=class{_tNode;_lView;constructor(n,e){this._tNode=n,this._lView=e}get(n,e,r){return ty(this._tNode,this._lView,n,Yr(r),e)}};function CS(){return new Qn(rt(),W())}function gn(t){return Wa(()=>{let n=t.prototype.constructor,e=n[ga]||sf(n),r=Object.prototype,i=Object.getPrototypeOf(t.prototype).constructor;for(;i&&i!==r;){let o=i[ga]||sf(i);if(o&&o!==e)return o;i=Object.getPrototypeOf(i)}return o=>new o})}function sf(t){return im(t)?()=>{let n=sf(ot(t));return n&&n()}:dr(t)}function DS(t,n,e,r,i){let o=t,a=n;for(;o!==null&&a!==null&&a[H]&2048&&!Ki(a);){let s=ny(o,a,e,r|2,An);if(s!==An)return s;r&=-5;let l=o.parent;if(!l){let c=a[bm];if(c){let d=c.get(e,An,r);if(d!==An)return d}l=ry(a),a=a[pr]}o=l}return i}function ry(t){let n=t[L],e=n.type;return e===2?n.declTNode:e===1?t[lt]:null}function Pf(t){return _S(rt(),t)}function S(t){return{token:t.token,providedIn:t.autoProvided===!1?null:"root",factory:t.factory,value:void 0}}function SS(){return ao(rt(),W())}function ao(t,n){return new V(dn(t,n))}var V=(()=>{class t{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=SS}return t})();function iy(t){return t instanceof V?t.nativeElement:t}function ES(){return this._results[Symbol.iterator]()}var _c=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new E}constructor(n=!1){this._emitDistinctChangesOnly=n}get(n){return this._results[n]}map(n){return this._results.map(n)}filter(n){return this._results.filter(n)}find(n){return this._results.find(n)}reduce(n,e){return this._results.reduce(n,e)}forEach(n){this._results.forEach(n)}some(n){return this._results.some(n)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(n,e){this.dirty=!1;let r=wb(n);(this._changesDetected=!xb(this._results,r,e))&&(this._results=r,this.length=r.length,this.last=r[this.length-1],this.first=r[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(n){this._onDirty=n}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=ES};function oy(t){return(t.flags&128)===128}var Lf=(function(t){return t[t.OnPush=0]="OnPush",t[t.Eager=1]="Eager",t[t.Default=1]="Default",t})(Lf||{}),ay=new Map,kS=0;function IS(){return kS++}function MS(t){ay.set(t[ka],t)}function lf(t){ay.delete(t[ka])}var bv="__ngContext__";function no(t,n){Yn(n)?(t[bv]=n[ka],MS(n)):t[bv]=n}function sy(t){return cy(t[qi])}function ly(t){return cy(t[$t])}function cy(t){for(;t!==null&&!Gt(t);)t=t[$t];return t}var cf;function Vf(t){cf=t}function dy(){if(cf!==void 0)return cf;if(typeof document<"u")return document;throw new D(210,!1)}var uy="r";var my="di";var fy=!1,py=new b("",{factory:()=>fy});var vv=new WeakMap;function NS(t,n){if(t==null||typeof t!="object")return;let e=vv.get(t);e||(e=new WeakSet,vv.set(t,e)),e.add(n)}var TS=(t,n,e,r)=>{};function AS(t,n,e,r){TS(t,n,e,r)}function Tc(t){return(t.flags&32)===32}var RS=()=>null;function hy(t,n,e=!1){return RS(t,n,e)}function gy(t,n){let e=t.contentQueries;if(e!==null){let r=O(null);try{for(let i=0;i<e.length;i+=2){let o=e[i],a=e[i+1];if(a!==-1){let s=t.data[a];Ta(o),s.contentQueries(2,n[a],a)}}}finally{O(r)}}}function df(t,n,e){Ta(0);let r=O(null);try{n(t,e)}finally{O(r)}}function by(t,n,e){if(ym(n)){let r=O(null);try{let i=n.directiveStart,o=n.directiveEnd;for(let a=i;a<o;a++){let s=t.data[a];if(s.contentQueries){let l=e[a];s.contentQueries(1,l,a)}}}finally{O(r)}}}var hn=(function(t){return t[t.Emulated=0]="Emulated",t[t.None=2]="None",t[t.ShadowDom=3]="ShadowDom",t[t.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",t})(hn||{}),uf=class{changingThisBreaksApplicationSecurity;constructor(n){this.changingThisBreaksApplicationSecurity=n}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity}`}};function ja(t){return t instanceof uf?t.changingThisBreaksApplicationSecurity:t}function OS(t,n){return t.createText(n)}function FS(t,n,e){t.setValue(n,e)}function vy(t,n,e){return t.createElement(n,e)}function di(t,n,e,r,i){t.insertBefore(n,e,r,i)}function yy(t,n,e){t.appendChild(n,e)}function yv(t,n,e,r,i){r!==null?di(t,n,e,r,i):yy(t,n,e)}function _y(t,n,e,r){t.removeChild(null,n,e,r)}function PS(t,n,e){t.setAttribute(n,"style",e)}function LS(t,n,e){e===""?t.removeAttribute(n,"class"):t.setAttribute(n,"class",e)}function xy(t,n,e){let{mergedAttrs:r,classes:i,styles:o}=e;r!==null&&pS(t,n,r),i!==null&&LS(t,n,i),o!==null&&PS(t,n,o)}function VS(t){return t instanceof Function?t():t}function jS(t,n,e){let r=t.length;for(;;){let i=t.indexOf(n,e);if(i===-1)return i;if(i===0||t.charCodeAt(i-1)<=32){let o=n.length;if(i+o===r||t.charCodeAt(i+o)<=32)return i}e=i+1}}var wy="ng-template";function BS(t,n,e,r){let i=0;if(r){for(;i<n.length&&typeof n[i]=="string";i+=2)if(n[i]==="class"&&jS(n[i+1].toLowerCase(),e,0)!==-1)return!0}else if(jf(t))return!1;if(i=n.indexOf(1,i),i>-1){let o;for(;++i<n.length&&typeof(o=n[i])=="string";)if(o.toLowerCase()===e)return!0}return!1}function jf(t){return t.type===4&&t.value!==wy}function zS(t,n,e){let r=t.type===4&&!e?wy:t.value;return n===r}function HS(t,n,e){let r=4,i=t.attrs,o=i!==null?GS(i):0,a=!1;for(let s=0;s<n.length;s++){let l=n[s];if(typeof l=="number"){if(!a&&!fn(r)&&!fn(l))return!1;if(a&&fn(l))continue;a=!1,r=l|r&1;continue}if(!a)if(r&4){if(r=2|r&1,l!==""&&!zS(t,l,e)||l===""&&n.length===1){if(fn(r))return!1;a=!0}}else if(r&8){if(i===null||!BS(t,i,l,e)){if(fn(r))return!1;a=!0}}else{let c=n[++s],d=US(l,i,jf(t),e);if(d===-1){if(fn(r))return!1;a=!0;continue}if(c!==""){let m;if(d>o?m="":m=i[d+1].toLowerCase(),r&2&&c!==m){if(fn(r))return!1;a=!0}}}}return fn(r)||a}function fn(t){return(t&1)===0}function US(t,n,e,r){if(n===null)return-1;let i=0;if(r||!e){let o=!1;for(;i<n.length;){let a=n[i];if(a===t)return i;if(a===3||a===6)o=!0;else if(a===1||a===2){let s=n[++i];for(;typeof s=="string";)s=n[++i];continue}else{if(a===4)break;if(a===0){i+=4;continue}}i+=o?1:2}return-1}else return WS(n,t)}function Cy(t,n,e=!1){for(let r=0;r<n.length;r++)if(HS(t,n[r],e))return!0;return!1}function $S(t){let n=t.attrs;if(n!=null){let e=n.indexOf(5);if((e&1)===0)return n[e+1]}return null}function GS(t){for(let n=0;n<t.length;n++){let e=t[n];if(Yv(e))return n}return t.length}function WS(t,n){let e=t.indexOf(4);if(e>-1)for(e++;e<t.length;){let r=t[e];if(typeof r=="number")return-1;if(r===n)return e;e++}return-1}function qS(t,n){e:for(let e=0;e<n.length;e++){let r=n[e];if(t.length===r.length){for(let i=0;i<t.length;i++)if(t[i]!==r[i])continue e;return!0}}return!1}function _v(t,n){return t?":not("+n.trim()+")":n}function YS(t){let n=t[0],e=1,r=2,i="",o=!1;for(;e<t.length;){let a=t[e];if(typeof a=="string")if(r&2){let s=t[++e];i+="["+a+(s.length>0?'="'+s+'"':"")+"]"}else r&8?i+="."+a:r&4&&(i+=" "+a);else i!==""&&!fn(a)&&(n+=_v(o,i),i=""),r=a,o=o||!fn(r);e++}return i!==""&&(n+=_v(o,i)),n}function KS(t){return t.map(YS).join(",")}function ZS(t){let n=[],e=[],r=1,i=2;for(;r<t.length;){let o=t[r];if(typeof o=="string")i===2?o!==""&&n.push(o,t[++r]):i===8&&e.push(o);else{if(!fn(i))break;i=o}r++}return e.length&&n.push(1,...e),n}var It={},Rn=(function(t){return t[t.Important=1]="Important",t[t.DashCase=2]="DashCase",t})(Rn||{}),QS;function Bf(t,n){return QS(t,n)}var zf=new Set;var i2=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var mf=new WeakMap;function Dy(t){return t?t[pr]??t:null}var Xi=new WeakSet;function XS(t,n,e){let r=mf.get(t);if(!r||r.length===0)return;let i=n.parentNode,o=n.previousSibling,a=Dy(e);for(let s=r.length-1;s>=0;s--){let{el:l,declarationView:c}=r[s],d=l.parentNode;l===n?(r.splice(s,1),Xi.add(l),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):o&&l===o?(r.splice(s,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l)):d&&i&&d!==i&&(a===null||c===null||a===c)&&(r.splice(s,1),l.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),l.parentNode?.removeChild(l))}}function JS(t,n,e){let r=Dy(e),i=mf.get(t);i?i.some(o=>o.el===n)||i.push({el:n,declarationView:r}):mf.set(t,[{el:n,declarationView:r}])}var Ac=(function(t){return t[t.CHANGE_DETECTION=0]="CHANGE_DETECTION",t[t.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",t})(Ac||{}),Pn=new b(""),xv=new Set;function _r(t){xv.has(t)||(xv.add(t),performance?.mark?.("mark_feature_usage",{detail:{feature:t}}))}var Rc=(()=>{class t{impl=null;execute(){this.impl?.execute()}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})(),Hf=[0,1,2,3],Uf=(()=>{class t{ngZone=u(T);scheduler=u(Wn);errorHandler=u(Sn,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(Pn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&be(pe.AfterRenderHooksStart),this.executing=!0;for(let r of Hf)for(let i of this.sequences)if(!(i.erroredOrDestroyed||!i.hooks[r]))try{i.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=i.hooks[r];return o(i.pipelinedValue)},i.snapshot))}catch(o){i.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let r of this.sequences)r.afterRun(),r.once&&(this.sequences.delete(r),r.destroy());for(let r of this.deferredRegistrations)this.sequences.add(r);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&be(pe.AfterRenderHooksEnd)}register(e){let{view:r}=e;r!==void 0?((r[ni]??=[]).push(e),ai(r),r[H]|=8192):this.addSequence(e)}addSequence(e){this.executing?this.deferredRegistrations.add(e):this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,r){return r?r.run(Ac.AFTER_NEXT_RENDER,e):e()}static \u0275prov=me({token:t,providedIn:"root",factory:()=>new t})}return t})(),Ba=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(n,e,r,i,o,a=null){this.impl=n,this.hooks=e,this.view=r,this.once=i,this.snapshot=a,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let n=this.view?.[ni];n&&(this.view[ni]=n.filter(e=>e!==this))}};function it(t,n){let e=n?.injector??u(J);return _r("NgAfterNextRender"),tE(t,e,n,!0)}function eE(t){return t instanceof Function?[void 0,void 0,t,void 0]:[t.earlyRead,t.write,t.mixedReadWrite,t.read]}function tE(t,n,e,r){let i=n.get(Rc);i.impl??=n.get(Uf);let o=n.get(Pn,null,{optional:!0}),a=e?.manualCleanup!==!0?n.get(at):null,s=n.get(Qi,null,{optional:!0}),l=new Ba(i.impl,eE(t),s?.view,r,a,o?.snapshot(null));return i.impl.register(l),l}var ff=null,Sy=null;function wv(t,n,e,r){ff?.maybeQueueEnterAnimation(t,n,e,r)}function Cv(t,n,e,r){ff===null?r(!1):ff.runLeaveAnimationsWithCallback(t,n,e,r)}function nE(t){Sy?.initViewDetachAnimations(t)}function rE(t){Sy?.clearViewDetachAnimations(t)}function Ji(t,n,e,r,i,o,a,s){if(i!=null){let l,c=!1;Gt(i)?l=i:Yn(i)&&(c=!0,i=i[cn]);let d=ct(i);t===0&&r!==null?(wv(s,r,o,e),a==null?yy(n,r,d):di(n,r,d,a||null,!0)):t===1&&r!==null?(di(n,r,d,a||null,!0),XS(o,d,s),Xi.has(d)||wv(s,r,o,e)):t===2?(s?.[Tb]?.leave?.has(o.index)&&JS(o,d,s),Xi.delete(d),Cv(s,o,e,m=>{if(Xi.has(d)){Xi.delete(d);return}_y(n,d,c,m)})):t===3&&(Xi.delete(d),Cv(s,o,e,()=>{n.destroyNode(d)})),l!=null&&pE(n,t,e,l,o,r,a)}}function iE(t,n){Ey(t,n),n[cn]=null,n[lt]=null}function oE(t,n,e,r,i,o){r[cn]=i,r[lt]=n,Fc(t,r,e,1,i,o)}function Ey(t,n){n[En].changeDetectionScheduler?.notify(9),Fc(t,n,n[Ee],2,null,null)}function aE(t){let n=t[qi];if(!n)return Ym(t[L],t);for(;n;){let e=null;if(Yn(n))e=n[qi];else{let r=n[We];r&&(e=r)}if(!e){for(;n&&!n[$t]&&n!==t;)Yn(n)&&Ym(n[L],n),n=n[Ke];n===null&&(n=t),Yn(n)&&Ym(n[L],n),e=n&&n[$t]}n=e}}function $f(t,n){let e=t[ii],r=e.indexOf(n);e.splice(r,1)}function Oc(t,n){if(oi(n))return;let e=n[Ee];e.destroyNode&&Fc(t,n,e,3,null,null),aE(n)}function Ym(t,n){if(oi(n))return;let e=O(null);try{n[H]&=-129,n[H]|=256,n[Lt]&&lr(n[Lt]),lE(t,n),sE(t,n),n[L].type===1&&n[Ee].destroy();let r=n[hr];if(r!==null&&Gt(n[Ke])){r!==n[Ke]&&$f(r,n);let i=n[kn];i!==null&&i.detachView(t)}lf(n)}finally{O(e)}}function sE(t,n){let e=t.cleanup,r=n[Wi];if(e!==null)for(let a=0;a<e.length-1;a+=2)if(typeof e[a]=="string"){let s=e[a+3];s>=0?r[s]():r[-s].unsubscribe(),a+=2}else{let s=r[e[a+1]];e[a].call(s)}r!==null&&(n[Wi]=null);let i=n[Un];if(i!==null){n[Un]=null;for(let a=0;a<i.length;a++){let s=i[a];s()}}let o=n[$n];if(o!==null){n[$n]=null;for(let a of o)a.destroy()}}function lE(t,n){let e;if(t!=null&&(e=t.destroyHooks)!=null)for(let r=0;r<e.length;r+=2){let i=n[e[r]];if(!(i instanceof ui)){let o=e[r+1];if(Array.isArray(o))for(let a=0;a<o.length;a+=2){let s=i[o[a]],l=o[a+1];be(pe.LifecycleHookStart,s,l);try{l.call(s)}finally{be(pe.LifecycleHookEnd,s,l)}}else{be(pe.LifecycleHookStart,i,o);try{o.call(i)}finally{be(pe.LifecycleHookEnd,i,o)}}}}}function ky(t,n,e){if(n===null)throw new D(510,!1);return cE(t,n.parent,e)}function cE(t,n,e){let r=n;for(;r!==null&&r.type&168;)n=r,r=n.parent;if(r===null)return e[cn];if(Kn(r)){let{encapsulation:i}=t.data[r.directiveStart+r.componentOffset];if(i===hn.None||i===hn.Emulated)return null}return dn(r,e)}function Iy(t,n,e){return uE(t,n,e)}function dE(t,n,e){return t.type&40?dn(t,e):null}var uE=dE,Dv;function Gf(t,n,e,r){let i=ky(t,r,n),o=n[Ee],a=r.parent||n[lt],s=Iy(a,r,n);if(i!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)yv(o,i,e[l],s,!1);else yv(o,i,e,s,!1);Dv!==void 0&&Dv(o,r,n,e,i)}function Pa(t,n){if(n!==null){let e=n.type;if(e&3)return dn(n,t);if(e&4)return pf(-1,t[n.index]);if(e&8){let r=n.child;if(r!==null)return Pa(t,r);{let i=t[n.index];return Gt(i)?pf(-1,i):ct(i)}}else{if(e&128)return Pa(t,n.next);if(e&32)return Bf(n,t)()||ct(t[n.index]);{let r=My(t,n);if(r!==null){if(Array.isArray(r))return r[0];let i=Gn(t[gt]);return Pa(i,r)}else return Pa(t,n.next)}}}return null}function My(t,n){if(n!==null){let r=t[gt][lt],i=n.projection;return r.projection[i]}return null}function pf(t,n){let e=We+t+1;if(e<n.length){let r=n[e],i=r[L].firstChild;if(i!==null)return Pa(r,i)}return n[ri]}function Wf(t,n,e,r,i,o,a){for(;e!=null;){let s=r[qn];if(e.type===128){e=e.next;continue}let l=r[e.index],c=e.type;if(a&&n===0&&(l&&no(ct(l),r),e.flags|=2),!Tc(e))if(c&8)Wf(t,n,e.child,r,i,o,!1),Ji(n,t,s,i,l,e,o,r);else if(c&32){let d=Bf(e,r),m;for(;m=d();)Ji(n,t,s,i,m,e,o,r);Ji(n,t,s,i,l,e,o,r)}else c&16?Ny(t,n,r,e,i,o):Ji(n,t,s,i,l,e,o,r);e=a?e.projectionNext:e.next}}function Fc(t,n,e,r,i,o){t.type===3?mE(e,r,n,i,o):Wf(e,r,t.firstChild,n,i,o,!1)}function mE(t,n,e,r,i){let a=e[L].firstChild,s=a.next,l=ct(e[a.index]),c=ct(e[s.index]),d=s.index+1,m=e[d];if(n===1||n===0)r!==null&&(m&&m.hasChildNodes()?di(t,r,m,i,!0):(di(t,r,l,i,!0),di(t,r,c,i,!0)));else if(n===2){if(m||(m=document.createDocumentFragment(),e[d]=m),l&&l.parentNode===m)return;let p=l;for(;p!==null;){let f=p.nextSibling;if(m.appendChild(p),p===c)break;p=f}}}function fE(t,n,e){let r=n[Ee],i=ky(t,e,n),o=e.parent||n[lt],a=Iy(o,e,n);Ny(r,0,n,e,i,a)}function Ny(t,n,e,r,i,o){let a=e[gt],l=a[lt].projection[r.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let d=l[c];Ji(n,t,e[qn],i,d,r,o,e)}else{let c=l,d=a[Ke];oy(r)&&(c.flags|=128),Wf(t,n,c,d,i,o,!0)}}function pE(t,n,e,r,i,o,a){let s=r[ri],l=ct(r);if(s!==l&&Ji(n,t,e,o,s,i,a),(r[H]&4)===0)for(let c=We;c<r.length;c++){let d=r[c];Fc(d[L],d,t,n,o,s)}}function hE(t,n,e,r,i){if(n)i?t.addClass(e,r):t.removeClass(e,r);else{let o=r.indexOf("-")===-1?void 0:Rn.DashCase;i==null?t.removeStyle(e,r,o):(typeof i=="string"&&i.endsWith("!important")&&(i=i.slice(0,-10),o|=Rn.Important),t.setStyle(e,r,i,o))}}function qf(t,n,e,r,i,o,a,s,l,c,d){let m=ze+r,p=m+i,f=gE(m,p),h=typeof c=="function"?c():c;return f[L]={type:t,blueprint:f,template:e,queries:null,viewQuery:s,declTNode:n,data:f.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:p,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof a=="function"?a():a,firstChild:null,schemas:l,consts:h,incompleteFirstPass:!1,ssrId:d}}function gE(t,n){let e=[];for(let r=0;r<n;r++)e.push(r<t?null:It);return e}function bE(t){let n=t.tView;return n===null||n.incompleteFirstPass?t.tView=qf(1,null,t.template,t.decls,t.vars,t.directiveDefs,t.pipeDefs,t.viewQuery,t.schemas,t.consts,t.id):n}function Yf(t,n,e,r,i,o,a,s,l,c,d){let m=n.blueprint.slice();return m[cn]=i,m[H]=r|4|128|8|64|1024,(c!==null||t&&t[H]&2048)&&(m[H]|=2048),Dm(m),m[Ke]=m[pr]=t,m[Ge]=e,m[En]=a||t&&t[En],m[Ee]=s||t&&t[Ee],m[qn]=l||t&&t[qn]||null,m[lt]=o,m[ka]=IS(),m[ei]=d,m[bm]=c,m[gt]=n.type==2?t[gt]:m,m}function vE(t,n,e){let r=dn(n,t),i=bE(e),o=t[En].rendererFactory,a=Kf(t,Yf(t,i,null,Ty(e),r,n,null,o.createRenderer(r,e),null,null,null));return t[n.index]=a}function Ty(t){let n=16;return t.signals?n=4096:t.onPush&&(n=64),n}function Ay(t,n,e,r){if(e===0)return-1;let i=n.length;for(let o=0;o<e;o++)n.push(r),t.blueprint.push(r),t.data.push(null);return i}function Kf(t,n){return t[qi]?t[gm][$t]=n:t[qi]=n,t[gm]=n,n}function _(t=1){Ry(Ve(),W(),Mn()+t,!1)}function Ry(t,n,e,r){if(!r)if((n[H]&3)===3){let o=t.preOrderCheckHooks;o!==null&&dc(n,o,e)}else{let o=t.preOrderHooks;o!==null&&uc(n,o,0,e)}br(e)}var Pc=(function(t){return t[t.None=0]="None",t[t.SignalBased=1]="SignalBased",t[t.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",t})(Pc||{});function hf(t,n,e,r){let i=O(null);try{let[o,a,s]=t.inputs[e],l=null;(a&Pc.SignalBased)!==0&&(l=n[o][et]),l!==null&&l.transformFn!==void 0?r=l.transformFn(r):s!==null&&(r=s.call(n,r)),t.setInput!==null?t.setInput(n,l,r,e,o):Uv(n,l,o,r)}finally{O(i)}}function Oy(t,n,e,r,i){let o=Mn(),a=r&2;try{br(-1),a&&n.length>ze&&Ry(t,n,ze,!1);let s=a?pe.TemplateUpdateStart:pe.TemplateCreateStart;be(s,i,e),e(r,i)}finally{br(o);let s=a?pe.TemplateUpdateEnd:pe.TemplateCreateEnd;be(s,i,e)}}function Zf(t,n,e){DE(t,n,e),(e.flags&64)===64&&SE(t,n,e)}function Lc(t,n,e=dn){let r=n.localNames;if(r!==null){let i=n.index+1;for(let o=0;o<r.length;o+=2){let a=r[o+1],s=a===-1?e(n,t):t[a];t[i++]=s}}}function yE(t,n,e,r){let o=r.get(py,fy)||e===hn.ShadowDom||e===hn.ExperimentalIsolatedShadowDom,a=t.selectRootElement(n,o);return _E(a),a}function _E(t){xE(t)}var xE=()=>null;function wE(t){return t==="class"?"className":t==="for"?"htmlFor":t==="formaction"?"formAction":t==="innerHtml"?"innerHTML":t==="readonly"?"readOnly":t==="tabindex"?"tabIndex":t}function Fy(t,n,e,r,i,o){let a=n[L];if(Qf(t,a,n,e,r)){Kn(t)&&CE(n,t.index);return}t.type&3&&(e=wE(e)),Py(t,n,e,r,i,o)}function Py(t,n,e,r,i,o){if(t.type&3){let a=dn(t,n);r=o!=null?o(r,t.value||"",e):r,r=Ly(e,a,r),i.setProperty(a,e,r)}else t.type&12}function Ly(t,n,e){return t==="src"&&(n.tagName==="IMG"||n.tagName==="VIDEO")?e==null?"":ja(e):e}function CE(t,n){let e=Wt(n,t);e[H]&16||(e[H]|=64)}function DE(t,n,e){let r=e.directiveStart,i=e.directiveEnd;Kn(e)&&vE(n,e,t.data[r+e.componentOffset]),t.firstCreatePass||yc(e,n);let o=e.initialInputs;for(let a=r;a<i;a++){let s=t.data[a],l=Va(n,t,a,e);if(no(l,n),o!==null&&ME(n,a-r,l,s,e,o),In(s)){let c=Wt(e.index,n);c[Ge]=Va(n,t,a,e)}}}function SE(t,n,e){let r=e.directiveStart,i=e.directiveEnd,o=e.index,a=Yb();try{br(o);for(let s=r;s<i;s++){let l=t.data[s],c=n[s];ec(s),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&EE(l,c)}}finally{br(-1),ec(a)}}function EE(t,n){t.hostBindings!==null&&t.hostBindings(1,n)}function Vy(t,n){let e=t.directiveRegistry,r=null;if(e)for(let i=0;i<e.length;i++){let o=e[i];Cy(n,o.selectors,!1)&&(r??=[],In(o)?r.unshift(o):r.push(o))}return r}function kE(t,n,e,r,i,o){let a=dn(t,n);r=Ly(e,a,r),IE(n[Ee],a,o,t.value,e,r,i)}function IE(t,n,e,r,i,o,a){if(o==null)a?.(o,r||"",i),t.removeAttribute(n,i,e);else{let s=a==null?Da(o):a(o,r||"",i);t.setAttribute(n,i,s,e)}}function ME(t,n,e,r,i,o){let a=o[n];if(a!==null)for(let s=0;s<a.length;s+=2){let l=a[s],c=a[s+1];hf(r,e,l,c)}}function jy(t,n,e,r,i){let o=ze+e,a=n[L],s=i(a,n,t,r,e);n[o]=s,Zi(t,!0);let l=t.type===2;return l?(xy(n[Ee],s,t),(jb()===0||Ma(t))&&no(s,n),Bb()):no(s,n),ic()&&(!l||!Tc(t))&&Gf(a,n,s,t),t}function By(t){let n=t;return Om()?Fm():(n=n.parent,Zi(n,!1)),n}function NE(t,n){let e=t[qn];if(!e)return;let r;try{r=e.get(mn,null)}catch{r=null}r?.(n)}function Qf(t,n,e,r,i){let o=t.inputs?.[r],a=t.hostDirectiveInputs?.[r],s=!1;if(a)for(let l=0;l<a.length;l+=2){let c=a[l],d=a[l+1],m=n.data[c];hf(m,e[c],d,i),s=!0}if(o)for(let l of o){let c=e[l],d=n.data[l];hf(d,c,r,i),s=!0}return s}function TE(t,n){let e=Wt(n,t),r=e[L];AE(r,e);let i=e[cn];i!==null&&e[ei]===null&&(e[ei]=hy(i,e[qn])),be(pe.ComponentStart);try{Xf(r,e,e[Ge])}finally{be(pe.ComponentEnd,e[Ge])}}function AE(t,n){for(let e=n.length;e<t.blueprint.length;e++)n.push(t.blueprint[e])}function Xf(t,n,e){nc(n);try{let r=t.viewQuery;r!==null&&df(1,r,e);let i=t.template;i!==null&&Oy(t,n,i,1,e),t.firstCreatePass&&(t.firstCreatePass=!1),n[kn]?.finishViewCreation(t),t.staticContentQueries&&gy(t,n),t.staticViewQueries&&df(2,t.viewQuery,e);let o=t.components;o!==null&&RE(n,o)}catch(r){throw t.firstCreatePass&&(t.incompleteFirstPass=!0,t.firstCreatePass=!1),r}finally{n[H]&=-5,rc()}}function RE(t,n){for(let e=0;e<n.length;e++)TE(t,n[e])}function qa(t,n,e,r){let i=O(null);try{let o=n.tView,s=t[H]&4096?4096:16,l=Yf(t,o,e,s,null,n,null,null,r?.injector??null,r?.embeddedViewInjector??null,r?.dehydratedView??null),c=t[n.index];l[hr]=c;let d=t[kn];return d!==null&&(l[kn]=d.createEmbeddedView(o)),Xf(o,l,e),l}finally{O(i)}}function ro(t,n){return!n||n.firstChild===null||oy(t)}function za(t,n,e,r,i=!1){if(t.type===3){let o=t.firstChild,a=o.next,s=ct(n[o.index]),l=ct(n[a.index]),c=s;for(;c!==null&&(r.push(c),c!==l);)c=c.nextSibling;return r}for(;e!==null;){if(e.type===128){e=i?e.projectionNext:e.next;continue}let o=n[e.index];if(o!==null)if(Gt(o)){let s=o[ri];s!==o[cn]&&r.push(ct(o)),o[H]&4||zy(o,r),r.push(s)}else r.push(ct(o));let a=e.type;if(a&8)za(t,n,e.child,r);else if(a&32){let s=Bf(e,n),l;for(;l=s();)r.push(l)}else if(a&16){let s=My(n,e);if(Array.isArray(s))r.push(...s);else{let l=Gn(n[gt]);za(l[L],l,s,r,!0)}}e=i?e.projectionNext:e.next}return r}function zy(t,n){for(let e=We;e<t.length;e++){let r=t[e],i=r[L].firstChild;i!==null&&za(r[L],r,i,n)}}function Hy(t){if(t[ni]!==null){for(let n of t[ni])n.impl.addSequence(n);t[ni].length=0}}var Uy=[];function OE(t){return t[Lt]??FE(t)}function FE(t){let n=Uy.pop()??Object.create(LE);return n.lView=t,n}function PE(t){t.lView[Lt]!==t&&(t.lView=null,Uy.push(t))}var LE=R(g({},Vr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{ai(t.lView)},consumerOnSignalRead(){this.lView[Lt]=this}});function VE(t){let n=t[Lt]??Object.create(jE);return n.lView=t,n}var jE=R(g({},Vr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:t=>{let n=Gn(t.lView);for(;n&&!$y(n[L]);)n=Gn(n);n&&Sm(n)},consumerOnSignalRead(){this.lView[Lt]=this}});function $y(t){return t.type!==2}function Gy(t){if(t[$n]===null)return;let n=!0;for(;n;){let e=!1;for(let r of t[$n])if(r.dirty&&(e=!0,r.zone===null||Zone.current===r.zone?r.run():r.zone.run(()=>r.run()),t[$n]===null))return;n=e&&!!(t[H]&8192)}}var BE=100;function Wy(t,n=0){let r=t[En].rendererFactory,i=!1;i||r.begin?.();try{zE(t,n)}finally{i||r.end?.()}}function zE(t,n){let e=Pm();try{ba(!0),gf(t,n);let r=0;for(;Na(t);){if(r===BE)throw new D(103,!1);r++,gf(t,1)}}finally{ba(e)}}function HE(t,n,e,r){if(oi(n))return;let i=n[H],o=!1,a=!1;nc(n);let s=!0,l=null,c=null;o||($y(t)?(c=OE(n),l=sr(c)):Ks()===null?(s=!1,c=VE(n),l=sr(c)):n[Lt]&&(lr(n[Lt]),n[Lt]=null));try{Dm(n),Gb(t.bindingStartIndex),e!==null&&Oy(t,n,e,2,r);let d=(i&3)===3;if(!o)if(d){let f=t.preOrderCheckHooks;f!==null&&dc(n,f,null)}else{let f=t.preOrderHooks;f!==null&&uc(n,f,0,null),Wm(n,0)}if(a||UE(n),Gy(n),qy(n,0),t.contentQueries!==null&&gy(t,n),!o)if(d){let f=t.contentCheckHooks;f!==null&&dc(n,f)}else{let f=t.contentHooks;f!==null&&uc(n,f,1),Wm(n,1)}GE(t,n);let m=t.components;m!==null&&Ky(n,m,0);let p=t.viewQuery;if(p!==null&&df(2,p,r),!o)if(d){let f=t.viewCheckHooks;f!==null&&dc(n,f)}else{let f=t.viewHooks;f!==null&&uc(n,f,2),Wm(n,2)}if(t.firstUpdatePass===!0&&(t.firstUpdatePass=!1),n[Yl]){for(let f of n[Yl])f();n[Yl]=null}o||(Hy(n),n[H]&=-73)}catch(d){let m=!1,p=d,f=n;for(;f!==null;){if(Gt(f)){f=f[Ke];continue}let h=f[Yi];if(h)try{let w=n[gt][Ge],I=w?.constructor,P={declarationInstance:w,declarationType:I,caughtBy:h},$=O(null);try{h(ov(p),P)}finally{O($)}m=!0;break}catch(w){p=w}f=f[Ke]}if(!m)throw o||ai(n),p}finally{c!==null&&(jr(c,l),s&&PE(c)),rc()}}function qy(t,n){for(let e=sy(t);e!==null;e=ly(e))for(let r=We;r<e.length;r++){let i=e[r];Yy(i,n)}}function UE(t){for(let n=sy(t);n!==null;n=ly(n)){if(!(n[H]&2))continue;let e=n[ii];for(let r=0;r<e.length;r++){let i=e[r];Sm(i)}}}function $E(t,n,e){be(pe.ComponentStart);let r=Wt(n,t);try{Yy(r,e)}finally{be(pe.ComponentEnd,r[Ge])}}function Yy(t,n){Zl(t)&&gf(t,n)}function gf(t,n){let r=t[L],i=t[H],o=t[Lt],a=!!(n===0&&i&16);if(a||=!!(i&64&&n===0),a||=!!(i&1024),a||=!!(o?.dirty&&Ai(o)),a||=!1,o&&(o.dirty=!1),t[H]&=-9217,a)HE(r,t,r.template,t[Ge]);else if(i&8192){let s=O(null);try{Gy(t),qy(t,1);let l=r.components;l!==null&&Ky(t,l,1),Hy(t)}finally{O(s)}}}function Ky(t,n,e){for(let r=0;r<n.length;r++)$E(t,n[r],e)}function GE(t,n){let e=t.hostBindingOpCodes;if(e!==null)try{for(let r=0;r<e.length;r++){let i=e[r];if(i<0)br(~i);else{let o=i,a=e[++r],s=e[++r];qb(a,o);let l=n[o];be(pe.HostBindingsUpdateStart,l);try{s(2,l)}finally{be(pe.HostBindingsUpdateEnd,l)}}}}finally{br(-1)}}function Jf(t,n){let e=Pm()?64:1088;for(t[En].changeDetectionScheduler?.notify(n);t;){t[H]|=e;let r=Gn(t);if(Ki(t)&&!r)return t;t=r}return null}function Zy(t,n,e,r){return[t,!0,0,n,null,r,null,e,null,null]}function Qy(t,n){let e=We+n;if(e<t.length)return t[e]}function Ya(t,n,e,r=!0){let i=n[L];if(WE(i,n,t,e),r){let a=pf(e,t),s=n[Ee],l=s.parentNode(t[ri]);l!==null&&oE(i,t[lt],s,n,l,a)}let o=n[ei];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function Xy(t,n){let e=Ha(t,n);return e!==void 0&&Oc(e[L],e),e}function Ha(t,n){if(t.length<=We)return;let e=We+n,r=t[e];if(r){let i=r[hr];i!==null&&i!==t&&$f(i,r),n>0&&(t[e-1][$t]=r[$t]);let o=Sa(t,We+n);iE(r[L],r);let a=o[kn];a!==null&&a.detachView(o[L]),r[Ke]=null,r[$t]=null,r[H]&=-129}return r}function WE(t,n,e,r){let i=We+r,o=e.length;r>0&&(e[i-1][$t]=n),r<o-We?(n[$t]=e[i],um(e,We+r,n)):(e.push(n),n[$t]=null),n[Ke]=e;let a=n[hr];a!==null&&e!==a&&Jy(a,n);let s=n[kn];s!==null&&s.insertView(t),Ql(n),n[H]|=128}function Jy(t,n){let e=t[ii],r=n[Ke];if(Yn(r))t[H]|=2;else{let i=r[Ke][gt];n[gt]!==i&&(t[H]|=2)}e===null?t[ii]=[n]:e.push(n)}var yr=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let n=this._lView,e=n[L];return za(e,n,e.firstChild,[])}constructor(n,e){this._lView=n,this._cdRefInjectingView=e}get context(){return this._lView[Ge]}set context(n){this._lView[Ge]=n}get destroyed(){return oi(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let n=this._lView[Ke];if(Gt(n)){let e=n[Ia],r=e?e.indexOf(this):-1;r>-1&&(Ha(n,r),Sa(e,r))}this._attachedToViewContainer=!1}Oc(this._lView[L],this._lView)}onDestroy(n){Xl(this._lView,n)}markForCheck(){Jf(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[H]&=-129}reattach(){Ql(this._lView),this._lView[H]|=128}detectChanges(){this._lView[H]|=1024,Wy(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new D(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let n=Ki(this._lView),e=this._lView[hr];e!==null&&!n&&$f(e,this._lView),Ey(this._lView[L],this._lView)}attachToAppRef(n){if(this._attachedToViewContainer)throw new D(902,!1);this._appRef=n;let e=Ki(this._lView),r=this._lView[hr];r!==null&&!e&&Jy(r,this._lView),Ql(this._lView)}};var On=(()=>{class t{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=qE;constructor(e,r,i){this._declarationLView=e,this._declarationTContainer=r,this.elementRef=i}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,r){return this.createEmbeddedViewImpl(e,r)}createEmbeddedViewImpl(e,r,i){let o=qa(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:r,dehydratedView:i});return new yr(o)}}return t})();function qE(){return Vc(rt(),W())}function Vc(t,n){return t.type&4?new On(n,t,ao(t,n)):null}function so(t,n,e,r,i){let o=t.data[n];if(o===null)o=YE(t,n,e,r,i),Wb()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=r,o.attrs=i;let a=zb();o.injectorIndex=a===null?-1:a.injectorIndex}return Zi(o,!0),o}function YE(t,n,e,r,i){let o=Rm(),a=Om(),s=a?o:o&&o.parent,l=t.data[n]=ZE(t,s,e,n,r,i);return KE(t,l,o,a),l}function KE(t,n,e,r){t.firstChild===null&&(t.firstChild=n),e!==null&&(r?e.child==null&&n.parent!==null&&(e.child=n):e.next===null&&(e.next=n,n.prev=e))}function ZE(t,n,e,r,i,o){let a=n?n.injectorIndex:-1,s=0;return Nm()&&(s|=128),{type:e,index:r,insertBeforeIndex:null,injectorIndex:a,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:s,providerIndexes:0,value:i,namespace:jm(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:n,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function QE(t){let n=t[vm]??[],r=t[Ke][Ee],i=[];for(let o of n)o.data[my]!==void 0?i.push(o):XE(o,r);t[vm]=i}function XE(t,n){let e=0,r=t.firstChild;if(r){let i=t.data[uy];for(;e<i;){let o=r.nextSibling;_y(n,r,!1),r=o,e++}}}var JE=()=>null,ek=()=>null;function xc(t,n){return JE(t,n)}function e_(t,n,e){return ek(t,n,e)}var t_=class{},Xe=class{},Te=class{destroyNode=null;static __NG_ELEMENT_ID__=()=>tk()};function tk(){let t=W(),n=rt(),e=Wt(n.index,t);return(Yn(e)?e:t)[Ee]}var n_=(()=>{class t{static \u0275prov=me({token:t,providedIn:"root",factory:()=>null})}return t})();function r_(t){return t.debugInfo?.className||t.type.name||null}var fc={},wc=class{injector;parentInjector;constructor(n,e){this.injector=n,this.parentInjector=e}get(n,e,r){let i=this.injector.get(n,fc,r);return i!==fc||e===fc?i:this.parentInjector.get(n,e,r)}};function nk(t,n,e){return t[n]=e}function Yt(t,n,e){if(e===It)return!1;let r=t[n];return Object.is(r,e)?!1:(t[n]=e,!0)}function rk(t,n,e,r){let i=Yt(t,n,e);return Yt(t,n+1,r)||i}function pc(t,n,e){return function r(i){let o=r.__ngNativeEl__;o!==void 0&&NS(i,o);let a=Kn(t)?Wt(t.index,n):n;Jf(a,5);let s=n[Ge],l=Sv(n,s,e,i),c=r.__ngNextListenerFn__;for(;c;)l=Sv(n,s,c,i)&&l,c=c.__ngNextListenerFn__;return l}}function Sv(t,n,e,r){let i=O(null);try{return be(pe.OutputStart,n,e),e(r)!==!1}catch(o){return NE(t,o),!1}finally{be(pe.OutputEnd,n,e),O(i)}}function i_(t,n,e,r,i,o,a,s){let l=Ma(t),c=!1,d=null;if(!r&&l&&(d=ok(n,e,o,t.index)),d!==null){let m=d.__ngLastListenerFn__||d;m.__ngNextListenerFn__=a,d.__ngLastListenerFn__=a,c=!0}else{let m=dn(t,e),p=r?r(m):m;AS(e,p,o,s),r||(s.__ngNativeEl__=m);let f=i.listen(p,o,s);if(!ik(o)){let h=r?w=>r(ct(w[t.index])):t.index;o_(h,n,e,o,s,f,!1)}}return c}function ik(t){return t.startsWith("animation")||t.startsWith("transition")}function ok(t,n,e,r){let i=t.cleanup;if(i!=null)for(let o=0;o<i.length-1;o+=2){let a=i[o];if(a===e&&i[o+1]===r){let s=n[Wi],l=i[o+2];return s&&s.length>l?s[l]:null}typeof a=="string"&&(o+=2)}return null}function o_(t,n,e,r,i,o,a){let s=n.firstCreatePass?km(n):null,l=Em(e),c=l.length;l.push(i,o),s&&s.push(r,t,c,(c+1)*(a?-1:1))}function Ev(t,n,e,r,i,o){let a=n[e],s=n[L],c=s.data[e].outputs[r],m=a[c].subscribe(o);o_(t.index,s,n,i,o,m,!0)}var bf=Symbol("BINDING");var fi=new b("");function Cc(t,n,e){let r=e?t.styles:null,i=e?t.classes:null,o=0;if(n!==null)for(let a=0;a<n.length;a++){let s=n[a];if(typeof s=="number")o=s;else if(o==1)i=jl(i,s);else if(o==2){let l=s,c=n[++a];r=jl(r,l+": "+c+";")}}e?t.styles=r:t.stylesWithoutHost=r,e?t.classes=i:t.classesWithoutHost=i}function Ze(t,n=0){let e=W();if(e===null)return X(t,n);let r=rt();return ty(r,e,ot(t),n)}function jc(){let t="invalid";throw new Error(t)}function a_(t,n,e,r,i){let o=r===null?null:{"":-1},a=i(t,e);if(a!==null){let s=a,l=null,c=null;for(let d of a)if(d.resolveHostDirectives!==null){[s,l,c]=d.resolveHostDirectives(a);break}lk(t,n,e,s,o,l,c)}o!==null&&r!==null&&ak(e,r,o)}function ak(t,n,e){let r=t.localNames=[];for(let i=0;i<n.length;i+=2){let o=e[n[i+1]];if(o==null)throw new D(-301,!1);r.push(n[i],o)}}function sk(t,n,e){n.componentOffset=e,(t.components??=[]).push(n.index)}function lk(t,n,e,r,i,o,a){let s=r.length,l=null;for(let p=0;p<s;p++){let f=r[p];l===null&&In(f)&&(l=f,sk(t,e,p)),af(yc(e,n),t,f.type)}pk(e,t.data.length,s),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let p=0;p<s;p++){let f=r[p];f.providersResolver&&f.providersResolver(f)}let c=!1,d=!1,m=Ay(t,n,s,null);s>0&&(e.directiveToIndex=new Map);for(let p=0;p<s;p++){let f=r[p];if(e.mergedAttrs=to(e.mergedAttrs,f.hostAttrs),dk(t,e,n,m,f),fk(m,f,i),a!==null&&a.has(f)){let[w,I]=a.get(f);e.directiveToIndex.set(f.type,[m,w+e.directiveStart,I+e.directiveStart])}else(o===null||!o.has(f))&&e.directiveToIndex.set(f.type,m);f.contentQueries!==null&&(e.flags|=4),(f.hostBindings!==null||f.hostAttrs!==null||f.hostVars!==0)&&(e.flags|=64);let h=f.type.prototype;!c&&(h.ngOnChanges||h.ngOnInit||h.ngDoCheck)&&((t.preOrderHooks??=[]).push(e.index),c=!0),!d&&(h.ngOnChanges||h.ngDoCheck)&&((t.preOrderCheckHooks??=[]).push(e.index),d=!0),m++}ck(t,e,o)}function ck(t,n,e){for(let r=n.directiveStart;r<n.directiveEnd;r++){let i=t.data[r];if(e===null||!e.has(i))kv(0,n,i,r),kv(1,n,i,r),Mv(n,r,!1);else{let o=e.get(i);Iv(0,n,o,r),Iv(1,n,o,r),Mv(n,r,!0)}}}function kv(t,n,e,r){let i=t===0?e.inputs:e.outputs;for(let o in i)if(Object.hasOwn(i,o)){let a;t===0?a=n.inputs??={}:a=n.outputs??={},a[o]??=[],a[o].push(r),s_(n,o)}}function Iv(t,n,e,r){let i=t===0?e.inputs:e.outputs;for(let o in i)if(Object.hasOwn(i,o)){let a=i[o],s;t===0?s=n.hostDirectiveInputs??={}:s=n.hostDirectiveOutputs??={},s[a]??=[],s[a].push(r,o),s_(n,a)}}function s_(t,n){n==="class"?t.flags|=8:n==="style"&&(t.flags|=16)}function Mv(t,n,e){let{attrs:r,inputs:i,hostDirectiveInputs:o}=t;if(r===null||!e&&i===null||e&&o===null||jf(t)){t.initialInputs??=[],t.initialInputs.push(null);return}let a=null,s=0;for(;s<r.length;){let l=r[s];if(l===0){s+=4;continue}else if(l===5){s+=2;continue}else if(typeof l=="number")break;if(!e&&Object.hasOwn(i,l)){let c=i[l];for(let d of c)if(d===n){a??=[],a.push(l,r[s+1]);break}}else if(e&&Object.hasOwn(o,l)){let c=o[l];for(let d=0;d<c.length;d+=2)if(c[d]===n){a??=[],a.push(c[d+1],r[s+1]);break}}s+=2}t.initialInputs??=[],t.initialInputs.push(a)}function dk(t,n,e,r,i){t.data[r]=i;let o=i.factory||(i.factory=dr(i.type,!0)),a=new ui(o,In(i),Ze,null);t.blueprint[r]=a,e[r]=a,uk(t,n,r,Ay(t,e,i.hostVars,It),i)}function uk(t,n,e,r,i){let o=i.hostBindings;if(o){let a=t.hostBindingOpCodes;a===null&&(a=t.hostBindingOpCodes=[]);let s=~n.index;mk(a)!=s&&a.push(s),a.push(e,r,o)}}function mk(t){let n=t.length;for(;n>0;){let e=t[--n];if(typeof e=="number"&&e<0)return e}return 0}function fk(t,n,e){if(e){if(n.exportAs)for(let r=0;r<n.exportAs.length;r++)e[n.exportAs[r]]=t;In(n)&&(e[""]=t)}}function pk(t,n,e){t.flags|=1,t.directiveStart=n,t.directiveEnd=n+e,t.providerIndexes=n}function l_(t,n,e,r,i,o,a,s){let l=n[L],c=l.consts,d=qt(c,a),m=so(l,t,e,r,d);return o&&a_(l,n,m,qt(c,s),i),m.mergedAttrs=to(m.mergedAttrs,m.attrs),m.attrs!==null&&Cc(m,m.attrs,!1),m.mergedAttrs!==null&&Cc(m,m.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,m),m}function c_(t,n){Wv(t,n),ym(n)&&t.queries.elementEnd(n)}function hk(t,n,e,r,i,o){let a=n.consts,s=qt(a,i),l=so(n,t,e,r,s);if(l.mergedAttrs=to(l.mergedAttrs,l.attrs),o!=null){let c=qt(a,o);l.localNames=[];for(let d=0;d<c.length;d+=2)l.localNames.push(c[d],-1)}return l.attrs!==null&&Cc(l,l.attrs,!1),l.mergedAttrs!==null&&Cc(l,l.mergedAttrs,!0),n.queries!==null&&n.queries.elementStart(n,l),l}var d_=typeof ShadowRoot<"u",gk=typeof Document<"u";function bk(t){return Object.keys(t).map(n=>{let[e,r,i]=t[n],o={propName:e,templateName:n,isSignal:(r&Pc.SignalBased)!==0};return i&&(o.transform=i),o})}function vk(t){return Object.keys(t).map(n=>({propName:t[n],templateName:n}))}function yk(t,n,e){let r=n instanceof Le?n:n?.injector;return r&&t.getStandaloneInjector!==null&&(r=t.getStandaloneInjector(r)||r),r?new wc(e,r):e}function _k(t){let n=t.get(Xe,null);if(n===null)throw new D(407,!1);let e=t.get(n_,null),r=t.get(Wn,null),i=t.get(Pn,null,{optional:!0});return{rendererFactory:n,sanitizer:e,changeDetectionScheduler:r,ngReflect:!1,tracingService:i}}function xk(t,n,e){let r=u_(t);return vy(n,r,r==="svg"?_m:r==="math"?Rb:e)}function wk(t){if((t&&"localName"in t&&typeof t.localName=="string"?t.localName:t?.tagName)?.toLowerCase()==="script")throw new D(905,!1)}function u_(t){return(t.selectors[0][0]||"div").toLowerCase()}var io=class{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=bk(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=vk(this.componentDef.outputs),this.cachedOutputs}constructor(n,e){this.componentDef=n,this.ngModule=e,this.componentType=n.type,this.selector=KS(n.selectors),this.ngContentSelectors=n.ngContentSelectors??[],this.isBoundToModule=!!e}create(n,e,r,i,o,a,s,l){be(pe.DynamicComponentStart);let c=O(null);try{let d=this.componentDef,m=yk(d,i||this.ngModule,n),p=_k(m),f=p.tracingService;return f&&f.componentCreate?f.componentCreate(r_(d),()=>this.createComponentRef(p,m,e,r,o,a,s,l)):this.createComponentRef(p,m,e,r,o,a,s,l)}finally{O(c)}}createComponentRef(n,e,r,i,o,a,s,l){let c=this.componentDef,d=Ck(i,c,a,o,l),m=n.rendererFactory.createRenderer(null,c),p=i?yE(m,i,c.encapsulation,e):xk(c,m,s??null);wk(p);let f=e.get(fi,null),h=Dk(p,()=>e.get(G,null)??dy());f&&f.addHost(h);let w=a?.some(Nv)||o?.some($=>typeof $!="function"&&$.bindings.some(Nv)),I=Yf(null,d,null,512|Ty(c),null,null,n,m,e,null,hy(p,e,!0));f&&d_&&h instanceof ShadowRoot&&Xl(I,()=>{f.removeHost(h)}),I[ze]=p,nc(I);let P=null;try{let $=l_(ze,I,2,"#host",()=>d.directiveRegistry,!0,0);xy(m,p,$),no(p,I),Zf(d,I,$),by(d,$,I),c_(d,$),r!==void 0&&Ek($,this.ngContentSelectors,r),P=Wt($.index,I),I[Ge]=P[Ge],Xf(d,I,null)}catch($){throw P!==null&&lf(P),lf(I),$}finally{be(pe.DynamicComponentEnd),rc()}return new Dc(this.componentType,I,!!w)}};function Ck(t,n,e,r,i){let o=t?["ng-version","22.2.2"]:ZS(n.selectors[0]),a=null,s=null,l=0;if(e)for(let m of e)l+=m[bf].requiredVars,m.create&&(m.targetIdx=0,(a??=[]).push(m)),m.update&&(m.targetIdx=0,(s??=[]).push(m));if(r)for(let m=0;m<r.length;m++){let p=r[m];if(typeof p!="function")for(let f of p.bindings){l+=f[bf].requiredVars;let h=m+1;f.create&&(f.targetIdx=h,(a??=[]).push(f)),f.update&&(f.targetIdx=h,(s??=[]).push(f))}}let c=[n];if(r)for(let m of r){let p=typeof m=="function"?m:m.type,f=zl(p);c.push(f)}return qf(0,null,Sk(a,s),1,l,c,null,null,null,[o],null)}function Dk(t,n){let e=t.getRootNode?.();return gk&&e instanceof Document?e.head:e&&d_&&e instanceof ShadowRoot?e:n().head}function Sk(t,n){return!t&&!n?null:e=>{if(e&1&&t)for(let r of t)r.create();if(e&2&&n)for(let r of n)r.update()}}function Nv(t){let n=t[bf].kind;return n==="input"||n==="twoWay"}var Dc=class extends t_{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(n,e,r){super(),this._rootLView=e,this._hasInputBindings=r,this._tNode=Kl(e[L],ze),this.location=ao(this._tNode,e),this.instance=Wt(this._tNode.index,e)[Ge],this.hostView=this.changeDetectorRef=new yr(e,void 0),this.componentType=n}setInput(n,e){this._hasInputBindings;let r=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(n)&&Object.is(this.previousInputValues.get(n),e))return;let i=this._rootLView,o=Qf(r,i[L],i,n,e);this.previousInputValues.set(n,e);let a=Wt(r.index,i);Jf(a,1)}get injector(){return new Qn(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(n){this.hostView.onDestroy(n)}};function Ek(t,n,e){let r=t.projection=[];for(let i=0;i<n.length;i++){let o=e[i];r.push(o!=null&&o.length?Array.from(o):null)}}var bt=(()=>{class t{static __NG_ELEMENT_ID__=kk}return t})();function kk(){let t=rt();return m_(t,W())}var vf=class t extends bt{_lContainer;_hostTNode;_hostLView;constructor(n,e,r){super(),this._lContainer=n,this._hostTNode=e,this._hostLView=r}get element(){return ao(this._hostTNode,this._hostLView)}get injector(){return new Qn(this._hostTNode,this._hostLView)}get parentInjector(){let n=Ff(this._hostTNode,this._hostLView);if(Kv(n)){let e=bc(n,this._hostLView),r=gc(n),i=e[L].data[r+8];return new Qn(i,e)}else return new Qn(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(n){let e=Tv(this._lContainer);return e!==null&&e[n]||null}get length(){return this._lContainer.length-We}createEmbeddedView(n,e,r){let i,o,a;typeof r=="number"?i=r:r!=null&&(i=r.index,o=r.injector,a=r.onError);let s=xc(this._lContainer,n.ssrId),l=n.createEmbeddedViewImpl(e||{},o,s);return a&&(l._lView[Yi]=a),this.insertImpl(l,i,ro(this._hostTNode,s)),l}createComponent(n,e,r,i,o,a,s){let l,c,d=e||{};l=d.index,r=d.injector,i=d.projectableNodes,o=d.environmentInjector||d.ngModuleRef,a=d.directives,s=d.bindings,c=d.onError;let m=new io(ur(n)),p=r||this.parentInjector;if(!o&&m.ngModule==null){let $=this.parentInjector.get(Le,null);$&&(o=$)}let f=ur(m.componentType??{}),h=xc(this._lContainer,f?.id??null),w=h?.firstChild??null,I=m.create(p,i,w,o,a,s,this._getHostElementNamespace());return c&&(I.hostView._lView[Yi]=c),this.insertImpl(I.hostView,l,ro(this._hostTNode,h)),I}_getHostElementNamespace(){if(this._hostTNode.type&2){let n=this._hostTNode.parent??this._hostLView[lt];return n!==null&&n.type&2&&typeof n.value=="string"&&n.value.toLowerCase()==="foreignobject"?null:n?.namespace??null}return this._hostTNode.namespace}insert(n,e){return this.insertImpl(n,e,!0)}insertImpl(n,e,r){let i=n._lView;if(Fb(i)){let s=this.indexOf(n);if(s!==-1)this.detach(s);else{let l=i[Ke],c=new t(l,l[lt],l[Ke]);c.detach(c.indexOf(n))}}let o=this._adjustIndex(e),a=this._lContainer;return Ya(a,i,o,r),n.attachToViewContainerRef(),um(Km(a),o,n),n}move(n,e){return this.insert(n,e)}indexOf(n){let e=Tv(this._lContainer);return e!==null?e.indexOf(n):-1}remove(n){let e=this._adjustIndex(n,-1),r=Ha(this._lContainer,e);r&&(Sa(Km(this._lContainer),e),Oc(r[L],r))}detach(n){let e=this._adjustIndex(n,-1),r=Ha(this._lContainer,e);return r&&Sa(Km(this._lContainer),e)!=null?new yr(r):null}_adjustIndex(n,e=0){return n??this.length+e}};function Tv(t){return t[Ia]}function Km(t){return t[Ia]||(t[Ia]=[])}function m_(t,n){let e,r=n[t.index];return Gt(r)?e=r:(e=Zy(r,n,null,t),n[t.index]=e,Kf(n,e)),Mk(e,n,t,r),new vf(e,t,n)}function Ik(t,n){let e=t[Ee],r=e.createComment(""),i=dn(n,t),o=e.parentNode(i);return di(e,o,r,e.nextSibling(i),!1),r}var Mk=Ak,Nk=()=>!1;function Tk(t,n,e){return Nk(t,n,e)}function Ak(t,n,e,r){if(t[ri])return;let i;e.type&8?i=ct(r):i=Ik(n,e),t[ri]=i}var yf=class t{queryList;matches=null;constructor(n){this.queryList=n}clone(){return new t(this.queryList)}setDirty(){this.queryList.setDirty()}},_f=class t{queries;constructor(n=[]){this.queries=n}createEmbeddedView(n){let e=n.queries;if(e!==null){let r=n.contentQueries!==null?n.contentQueries[0]:e.length,i=[];for(let o=0;o<r;o++){let a=e.getByIndex(o),s=this.queries[a.indexInDeclarationView];i.push(s.clone())}return new t(i)}return null}insertView(n){this.dirtyQueriesWithMatches(n)}detachView(n){this.dirtyQueriesWithMatches(n)}finishViewCreation(n){this.dirtyQueriesWithMatches(n)}dirtyQueriesWithMatches(n){for(let e=0;e<this.queries.length;e++)tp(n,e).matches!==null&&this.queries[e].setDirty()}},Sc=class{flags;read;predicate;constructor(n,e,r=null){this.flags=e,this.read=r,typeof n=="string"?this.predicate=Lk(n):this.predicate=n}},xf=class t{queries;constructor(n=[]){this.queries=n}elementStart(n,e){for(let r=0;r<this.queries.length;r++)this.queries[r].elementStart(n,e)}elementEnd(n){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(n)}embeddedTView(n){let e=null;for(let r=0;r<this.length;r++){let i=e!==null?e.length:0,o=this.getByIndex(r).embeddedTView(n,i);o&&(o.indexInDeclarationView=r,e!==null?e.push(o):e=[o])}return e!==null?new t(e):null}template(n,e){for(let r=0;r<this.queries.length;r++)this.queries[r].template(n,e)}getByIndex(n){return this.queries[n]}get length(){return this.queries.length}track(n){this.queries.push(n)}},wf=class t{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(n,e=-1){this.metadata=n,this._declarationNodeIndex=e}elementStart(n,e){this.isApplyingToNode(e)&&this.matchTNode(n,e)}elementEnd(n){this._declarationNodeIndex===n.index&&(this._appliesToNextNode=!1)}template(n,e){this.elementStart(n,e)}embeddedTView(n,e){return this.isApplyingToNode(n)?(this.crossesNgTemplate=!0,this.addMatch(-n.index,e),new t(this.metadata)):null}isApplyingToNode(n){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,r=n.parent;for(;r!==null&&r.type&8&&r.index!==e;)r=r.parent;return e===(r!==null?r.index:-1)}return this._appliesToNextNode}matchTNode(n,e){let r=this.metadata.predicate;if(Array.isArray(r))for(let i=0;i<r.length;i++){let o=r[i];this.matchTNodeWithReadOption(n,e,Rk(e,o)),this.matchTNodeWithReadOption(n,e,mc(e,n,o,!1,!1))}else r===On?e.type&4&&this.matchTNodeWithReadOption(n,e,-1):this.matchTNodeWithReadOption(n,e,mc(e,n,r,!1,!1))}matchTNodeWithReadOption(n,e,r){if(r!==null){let i=this.metadata.read;if(i!==null)if(i===V||i===bt||i===J||i===On&&e.type&4)this.addMatch(e.index,-2);else{let o=mc(e,n,i,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,r)}}addMatch(n,e){this.matches===null?this.matches=[n,e]:this.matches.push(n,e)}};function Rk(t,n){let e=t.localNames;if(e!==null){for(let r=0;r<e.length;r+=2)if(e[r]===n)return e[r+1]}return null}function Ok(t,n){return t.type&11?ao(t,n):t.type&4?Vc(t,n):null}function Fk(t,n,e,r){return e===-1?Ok(n,t):e===-2?Pk(t,n,r):Va(t,t[L],e,n)}function Pk(t,n,e){if(e===V)return ao(n,t);if(e===On)return Vc(n,t);if(e===bt)return m_(n,t);if(e===J)return new Qn(n,t)}function f_(t,n,e,r){let i=n[kn].queries[r];if(i.matches===null){let o=t.data,a=e.matches,s=[];for(let l=0;a!==null&&l<a.length;l+=2){let c=a[l];if(c<0)s.push(null);else{let d=o[c];s.push(Fk(n,d,a[l+1],e.metadata.read))}}i.matches=s}return i.matches}function Cf(t,n,e,r){let i=t.queries.getByIndex(e),o=i.matches;if(o!==null){let a=f_(t,n,i,e);for(let s=0;s<o.length;s+=2){let l=o[s];if(l>0)r.push(a[s/2]);else{let c=o[s+1],d=n[-l];for(let m=We;m<d.length;m++){let p=d[m];p[hr]===p[Ke]&&Cf(p[L],p,c,r)}if(d[ii]!==null){let m=d[ii];for(let p=0;p<m.length;p++){let f=m[p];Cf(f[L],f,c,r)}}}}}return r}function ep(t,n){return t[kn].queries[n].queryList}function p_(t,n,e){let r=new _c((e&4)===4);return Vb(t,n,r,r.destroy),(n[kn]??=new _f).queries.push(new yf(r))-1}function h_(t,n,e){let r=Ve();return r.firstCreatePass&&(b_(r,new Sc(t,n,e),-1),(n&2)===2&&(r.staticViewQueries=!0)),p_(r,W(),n)}function g_(t,n,e,r){let i=Ve();if(i.firstCreatePass){let o=rt();b_(i,new Sc(n,e,r),o.index),Vk(i,t),(e&2)===2&&(i.staticContentQueries=!0)}return p_(i,W(),e)}function Lk(t){return t.split(",").map(n=>n.trim())}function b_(t,n,e){t.queries===null&&(t.queries=new xf),t.queries.track(new wf(n,e))}function Vk(t,n){let e=t.contentQueries||(t.contentQueries=[]),r=e.length?e[e.length-1]:-1;n!==r&&e.push(t.queries.length-1,n)}function tp(t,n){return t.queries.getByIndex(n)}function v_(t,n){let e=t[L],r=tp(e,n);return r.crossesNgTemplate?Cf(e,t,n,[]):f_(e,t,r,n)}function y_(t,n,e){let r,i=oa(()=>{r._dirtyCounter();let o=jk(r,t);if(n&&o===void 0)throw new D(-951,!1);return o});return r=i[et],r._dirtyCounter=A(0),r._flatValue=void 0,i}function np(t){return y_(!0,!1,t)}function rp(t){return y_(!0,!0,t)}function __(t,n){let e=t[et];e._lView=W(),e._queryIndex=n,e._queryList=ep(e._lView,n),e._queryList.onDirty(()=>e._dirtyCounter.update(r=>r+1))}function jk(t,n){let e=t._lView,r=t._queryIndex;if(e===void 0||r===void 0||e[H]&4)return n?void 0:ht;let i=ep(e,r),o=v_(e,r);return i.reset(o,iy),n?i.first:i._changesDetected||t._flatValue===void 0?t._flatValue=i.toArray():t._flatValue}function Xn(t){return!!t&&typeof t.then=="function"}function ip(t){return!!t&&typeof t.subscribe=="function"}var Fn=class{},Bc=class{};var Ec=class extends Fn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];constructor(n,e,r,i=!0){super(),this.ngModuleType=n,this._parent=e;let o=gb(n);this._bootstrapComponents=VS(o.bootstrap),this._r3Injector=Bm(n,e,[{provide:Fn,useValue:this},...r],_a(n),new Set(["environment"])),i&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let n=this._r3Injector;!n.destroyed&&n.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(n){this.destroyCbs.push(n)}},kc=class extends Bc{moduleType;constructor(n){super(),this.moduleType=n}create(n){return new Ec(this.moduleType,n,[])}};var Ua=class extends Fn{injector;instance=null;constructor(n){super();let e=new Zr([...n.providers,{provide:Fn,useValue:this}],n.parent||Gi(),n.debugName,new Set(["environment"]));this.injector=e,n.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(n){this.injector.onDestroy(n)}};function Ka(t,n,e=null){return new Ua({providers:t,parent:n,debugName:e,runEnvironmentInitializers:!0}).injector}var Bk=(()=>{class t{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let r=fm(!1,e.type),i=r.length>0?Ka([r],this._injector,""):null;this.cachedInjectors.set(e,i)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=me({token:t,providedIn:"environment",factory:()=>new t(X(Le))})}return t})();function k(t){return Wa(()=>{let n=x_(t),e=R(g({},n),{decls:t.decls,vars:t.vars,template:t.template,consts:t.consts||null,ngContentSelectors:t.ngContentSelectors,onPush:t.changeDetection!==Lf.Eager,directiveDefs:null,pipeDefs:null,dependencies:n.standalone&&t.dependencies||null,getStandaloneInjector:n.standalone?i=>i.get(Bk).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:t.signals??!1,data:t.data||{},encapsulation:t.encapsulation||hn.Emulated,styles:t.styles||ht,_:null,schemas:t.schemas||null,tView:null,id:""});n.standalone&&_r("NgStandalone"),w_(e);let r=t.dependencies;return e.directiveDefs=Av(r,zk),e.pipeDefs=Av(r,bb),e.id=$k(e),e})}function zk(t){return ur(t)||zl(t)}function oe(t){return Wa(()=>({type:t.type,bootstrap:t.bootstrap||ht,declarations:t.declarations||ht,imports:t.imports||ht,exports:t.exports||ht,transitiveCompileScopes:null,schemas:t.schemas||null,id:t.id||null}))}function Hk(t,n){if(t==null)return mr;let e={};for(let r in t)if(Object.hasOwn(t,r)){let i=t[r],o,a,s,l;Array.isArray(i)?(s=i[0],o=i[1],a=i[2]??o,l=i[3]||null):(o=i,a=i,s=Pc.None,l=null),e[o]=[r,s,l],n[o]=a}return e}function Uk(t){if(t==null)return mr;let n={};for(let e in t)Object.hasOwn(t,e)&&(n[t[e]]=e);return n}function j(t){return Wa(()=>{let n=x_(t);return w_(n),n})}function op(t){return{type:t.type,name:t.name,factory:null,pure:t.pure!==!1,standalone:t.standalone??!0,onDestroy:t.type.prototype.ngOnDestroy||null}}function x_(t){let n={};return{type:t.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:t.hostBindings||null,hostVars:t.hostVars||0,hostAttrs:t.hostAttrs||null,contentQueries:t.contentQueries||null,declaredInputs:n,inputConfig:t.inputs||mr,exportAs:t.exportAs||null,standalone:t.standalone??!0,signals:t.signals===!0,selectors:t.selectors||ht,viewQuery:t.viewQuery||null,features:t.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,signalFormsInputPresence:null,inputs:Hk(t.inputs,n),outputs:Uk(t.outputs),debugInfo:null}}function w_(t){t.features?.forEach(n=>n(t))}function Av(t,n){return t?()=>{let e=typeof t=="function"?t():t,r=[];for(let i of e){let o=n(i);o!==null&&r.push(o)}return r}:null}function $k(t){let n=0,e=typeof t.consts=="function"?"":t.consts,r=[t.selectors,t.ngContentSelectors,t.hostVars,t.hostAttrs,e,t.vars,t.decls,t.encapsulation,t.standalone,t.signals,t.exportAs,JSON.stringify(t.inputs),JSON.stringify(t.outputs),Object.getOwnPropertyNames(t.type.prototype),!!t.contentQueries,!!t.viewQuery];for(let o of r.join("|"))n=Math.imul(31,n)+o.charCodeAt(0)<<0;return n+=2147483648,"c"+n}var C_=new b("");var ap=(()=>{class t{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,r)=>{this.resolve=e,this.reject=r});appInits=u(C_,{optional:!0})??[];injector=u(J);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let i of this.appInits){let o=st(this.injector,i);if(Xn(o))e.push(o);else if(ip(o)){let a=new Promise((s,l)=>{o.subscribe({complete:s,error:l})});e.push(a)}}let r=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{r()}).catch(i=>{this.reject(i)}),e.length===0&&r(),this.initialized=!0}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function Gk(t){return Object.getPrototypeOf(t.prototype).constructor}function nt(t){let n=Gk(t.type),e=!0,r=[t];for(;n&&n!==Function.prototype&&n!==Object.prototype;){let i,o=Object.hasOwn(n,wa)?n[wa]:void 0,a=Object.hasOwn(n,Ca)?n[Ca]:void 0;if(In(t))i=o??a;else{if(o)throw new D(903,!1);i=a}if(i){if(e){r.push(i);let l=t;l.inputs=Zm(t.inputs),l.declaredInputs=Zm(t.declaredInputs),l.outputs=Zm(t.outputs);let c=i.hostBindings;c&&Zk(t,c);let d=i.viewQuery,m=i.contentQueries;if(d&&Yk(t,d),m&&Kk(t,m),Wk(t,i),hb(t.outputs,i.outputs),In(i)&&i.data.animation){let p=t.data;p.animation=(p.animation||[]).concat(i.data.animation)}}let s=i.features;if(s)for(let l=0;l<s.length;l++){let c=s[l];c&&c.ngInherit&&c(t),c===nt&&(e=!1)}}n=Object.getPrototypeOf(n)}qk(r)}function Wk(t,n){for(let e in n.inputs){if(!Object.hasOwn(n.inputs,e)||Object.hasOwn(t.inputs,e))continue;let r=n.inputs[e];r!==void 0&&(t.inputs[e]=r,t.declaredInputs[e]=n.declaredInputs[e])}}function qk(t){let n=0,e=null;for(let r=t.length-1;r>=0;r--){let i=t[r];i.hostVars=n+=i.hostVars,i.hostAttrs=to(i.hostAttrs,e=to(e,i.hostAttrs))}}function Zm(t){return t===mr?{}:t===ht?[]:t}function Yk(t,n){let e=t.viewQuery;e?t.viewQuery=(r,i)=>{n(r,i),e(r,i)}:t.viewQuery=n}function Kk(t,n){let e=t.contentQueries;e?t.contentQueries=(r,i,o)=>{n(r,i,o),e(r,i,o)}:t.contentQueries=n}function Zk(t,n){let e=t.hostBindings;e?t.hostBindings=(r,i)=>{n(r,i),e(r,i)}:t.hostBindings=n}function D_(t,n,e,r,i,o,a,s){if(e.firstCreatePass){t.mergedAttrs=to(t.mergedAttrs,t.attrs);let d=t.tView=qf(2,t,i,o,a,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,t),d.queries=e.queries.embeddedTView(t))}s&&(t.flags|=s),Zi(t,!1);let l=Xk(e,n,t,r);ic()&&Gf(e,n,l,t),no(l,n);let c=Zy(l,n,l,t);n[r+ze]=c,Kf(n,c),Tk(c,t,n)}function Qk(t,n,e,r,i,o,a,s,l,c,d){let m=e+ze,p;return n.firstCreatePass?(p=so(n,m,4,a||null,s||null),Mm()&&a_(n,t,p,qt(n.consts,c),Vy),Wv(n,p)):p=n.data[m],D_(p,t,n,e,r,i,o,l),Ma(p)&&Zf(n,t,p),c!=null&&Lc(t,p,d),p}function $a(t,n,e,r,i,o,a,s,l,c,d){let m=e+ze,p;if(n.firstCreatePass){if(p=so(n,m,4,a||null,s||null),c!=null){let f=qt(n.consts,c);p.localNames=[];for(let h=0;h<f.length;h+=2)p.localNames.push(f[h],-1)}}else p=n.data[m];return D_(p,t,n,e,r,i,o,l),c!=null&&Lc(t,p,d),p}function Jn(t,n,e,r,i,o,a,s){let l=W(),c=Ve(),d=qt(c.consts,o);return Qk(l,c,t,n,e,r,i,d,void 0,a,s),Jn}var Xk=Jk;function Jk(t,n,e,r){return oc(!0),n[Ee].createComment("")}var sp=(()=>{class t{log(e){console.log(e)}warn(e){console.warn(e)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"platform"})}return t})();var lp=new b("");var zc=new b("");function S_(){Eu(t=>{let n="";throw new D(-600,n)})}var eI=10;var Kt=(()=>{class t{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(mn);afterRenderManager=u(Rc);zonelessEnabled=u(Oa);rootEffectScheduler=u(sc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new E;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(vr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(_e(e=>!e))}constructor(){u(Pn,{optional:!0})}whenStable(){let e;return new Promise(r=>{e=this.isStable.subscribe({next:i=>{i&&r()}})}).finally(()=>{e.unsubscribe()})}_injector=u(Le);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,r){return this.bootstrapImpl(e,r)}bootstrapImpl(e,r,i=J.NULL){return this._injector.get(T).run(()=>{if(be(pe.BootstrapComponentStart),!this._injector.get(ap).done){let P="";throw new D(405,P)}let s=ur(e),l=this._injector.get(Fn),c=new io(s,l);this.componentTypes.push(e);let{hostElement:d,directives:m,bindings:p}=tI(r),f=d||c.selector,h=c.create(i,[],f,l.injector,m,p),w=h.location.nativeElement,I=h.injector.get(lp,null);return I?.registerApplication(w),h.onDestroy(()=>{this.detachView(h.hostView),La(this.components,h),I?.unregisterApplication(w)}),this._loadComponent(h),be(pe.BootstrapComponentEnd,h),h})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){be(pe.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Ac.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw be(pe.ChangeDetectionEnd),new D(101,!1);let e=O(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,O(e),this.afterTick.next(),be(pe.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(Xe,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<eI;){be(pe.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{be(pe.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let r=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:i}of this.allViews){if(!r&&!Na(i))continue;let o=r&&!this.zonelessEnabled?0:1;Wy(i,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Na(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let r=e;this._views.push(r),r.attachToAppRef(this)}detachView(e){let r=e;La(this._views,r),r.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(i){this.internalErrorHandler(i)}this.components.push(e),this._injector.get(zc,[]).forEach(i=>i(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>La(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new D(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function tI(t){return t===void 0||typeof t=="string"||t instanceof Element?{hostElement:t}:t}function La(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function de(t,n,e,r){let i=W(),o=gr();if(Yt(i,o,n)){let a=Ve(),s=Aa();kE(s,i,t,n,e,r)}return de}var Df=class{destroy(n){}updateValue(n,e){}swap(n,e){let r=Math.min(n,e),i=Math.max(n,e),o=this.detach(i);if(i-r>1){let a=this.detach(r);this.attach(r,o),this.attach(i,a)}else this.attach(r,o)}move(n,e){this.attach(e,this.detach(n))}};function Qm(t,n,e,r,i){return t===e&&Object.is(n,r)?1:Object.is(i(t,n),i(e,r))?-1:0}function nI(t,n,e,r){let i,o,a=0,s=t.length-1,l=void 0;if(Array.isArray(n)){O(r);let c=n.length-1;for(O(null);a<=s&&a<=c;){let d=t.at(a),m=n[a],p=Qm(a,d,a,m,e);if(p!==0){p<0&&t.updateValue(a,m),a++;continue}let f=t.at(s),h=n[c],w=Qm(s,f,c,h,e);if(w!==0){w<0&&t.updateValue(s,h),s--,c--;continue}let I=e(a,d),P=e(s,f),$=e(a,m);if(Object.is($,P)){let Je=e(c,h);Object.is(Je,I)?(t.swap(a,s),t.updateValue(s,h),c--,s--):t.move(s,a),t.updateValue(a,m),a++;continue}if(i??=new Ic,o??=Ov(t,a,s,e),Sf(t,i,a,$))t.updateValue(a,m),a++,s++;else if(o.has($))i.set(I,t.detach(a)),s--;else{let Je=t.create(a,n[a]);t.attach(a,Je),a++,s++}}for(;a<=c;)Rv(t,i,e,a,n[a]),a++}else if(n!=null){O(r);let c=n[Symbol.iterator]();O(null);let d=c.next();for(;!d.done&&a<=s;){let m=t.at(a),p=d.value,f=Qm(a,m,a,p,e);if(f!==0)f<0&&t.updateValue(a,p),a++,d=c.next();else{i??=new Ic,o??=Ov(t,a,s,e);let h=e(a,p);if(Sf(t,i,a,h))t.updateValue(a,p),a++,s++,d=c.next();else if(!o.has(h))t.attach(a,t.create(a,p)),a++,s++,d=c.next();else{let w=e(a,m);i.set(w,t.detach(a)),s--}}}for(;!d.done;)Rv(t,i,e,t.length,d.value),d=c.next()}for(;a<=s;)t.destroy(t.detach(s--));i?.forEach(c=>{t.destroy(c)})}function Sf(t,n,e,r){return n!==void 0&&n.has(r)?(t.attach(e,n.get(r)),n.delete(r),!0):!1}function Rv(t,n,e,r,i){if(Sf(t,n,r,e(r,i)))t.updateValue(r,i);else{let o=t.create(r,i);t.attach(r,o)}}function Ov(t,n,e,r){let i=new Set;for(let o=n;o<=e;o++)i.add(r(o,t.at(o)));return i}var Ic=class{kvMap=new Map;_vMap=void 0;has(n){return this.kvMap.has(n)}delete(n){if(!this.has(n))return!1;let e=this.kvMap.get(n);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(n,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(n),!0}get(n){return this.kvMap.get(n)}set(n,e){if(this.kvMap.has(n)){let r=this.kvMap.get(n);this._vMap===void 0&&(this._vMap=new Map);let i=this._vMap;for(;i.has(r);)r=i.get(r);i.set(r,e)}else this.kvMap.set(n,e)}forEach(n){for(let[e,r]of this.kvMap)if(n(r,e),this._vMap!==void 0){let i=this._vMap;for(;i.has(r);)r=i.get(r),n(r,e)}}};function xe(t,n,e,r,i,o,a,s){_r("NgControlFlow");let l=W(),c=Ve(),d=qt(c.consts,o);return $a(l,c,t,n,e,r,i,d,256,a,s),cp}function cp(t,n,e,r,i,o,a,s){return _r("NgControlFlow"),rI(t,n,e,r,i,o,a,s),cp}function rI(t,n,e,r,i,o,a,s){let l=W(),c=Ve(),d=qt(c.consts,o);$a(l,c,t,n,e,r,i,d,512,a,s)}function we(t,n){_r("NgControlFlow");let e=W(),r=gr(),i=e[r]!==It?e[r]:-1,o=i!==-1?Mc(e,ze+i):void 0,a=0;if(Yt(e,r,t)){let s=O(null);try{if(o!==void 0&&Xy(o,a),t!==-1){let l=ze+t,c=Mc(e,l),d=Mf(e[L],l),m=e_(c,d,e),p=qa(e,d,n,{dehydratedView:m});Ya(c,p,a,ro(d,m))}}finally{O(s)}}else if(o!==void 0){let s=Qy(o,a);s!==void 0&&(s[Ge]=n)}}var Ef=class{lContainer;$implicit;$index;constructor(n,e,r){this.lContainer=n,this.$implicit=e,this.$index=r}get $count(){return this.lContainer.length-We}};function dp(t,n){return n}var kf=class{hasEmptyBlock;trackByFn;liveCollection;constructor(n,e,r){this.hasEmptyBlock=n,this.trackByFn=e,this.liveCollection=r}};function pi(t,n,e,r,i,o,a,s,l,c,d,m,p){_r("NgControlFlow");let f=W(),h=Ve(),w=l!==void 0,I=W(),P=s?a.bind(I[gt][Ge]):a,$=new kf(w,P);I[ze+t]=$,$a(f,h,t+1,n,e,r,i,qt(h.consts,o),256),w&&$a(f,h,t+2,l,c,d,m,qt(h.consts,p),512)}var If=class extends Df{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(n,e,r){super(),this.lContainer=n,this.hostLView=e,this.templateTNode=r}get length(){return this.lContainer.length-We}at(n){return this.getLView(n)[Ge].$implicit}attach(n,e){let r=e[ei];this.needsIndexUpdate||=n!==this.length,Ya(this.lContainer,e,n,ro(this.templateTNode,r)),iI(this.lContainer,n)}detach(n){return this.needsIndexUpdate||=n!==this.length-1,oI(this.lContainer,n),aI(this.lContainer,n)}create(n,e){let r=xc(this.lContainer,this.templateTNode.tView.ssrId);return qa(this.hostLView,this.templateTNode,new Ef(this.lContainer,e,n),{dehydratedView:r})}destroy(n){Oc(n[L],n)}updateValue(n,e){this.getLView(n)[Ge].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let n=0;n<this.length;n++)this.getLView(n)[Ge].$index=n}getLView(n){return sI(this.lContainer,n)}};function hi(t){let n=O(null),e=Mn();try{let r=W(),i=r[L],o=r[e],a=e+1,s=Mc(r,a);if(o.liveCollection===void 0){let c=Mf(i,a);o.liveCollection=new If(s,r,c)}else o.liveCollection.reset();let l=o.liveCollection;if(nI(l,t,o.trackByFn,n),l.updateIndexes(),o.hasEmptyBlock){let c=gr(),d=l.length===0;if(Yt(r,c,d)){let m=e+2,p=Mc(r,m);if(d){let f=Mf(i,m),h=e_(p,f,r),w=qa(r,f,void 0,{dehydratedView:h});Ya(p,w,0,ro(f,h))}else i.firstUpdatePass&&QE(p),Xy(p,0)}}}finally{O(n)}}function Mc(t,n){return t[n]}function iI(t,n){if(t.length<=We)return;let e=We+n,r=t[e];r&&rE(r)}function oI(t,n){if(t.length<=We)return;let e=We+n,r=t[e];r&&nE(r)}function aI(t,n){return Ha(t,n)}function sI(t,n){return Qy(t,n)}function Mf(t,n){return Kl(t,n)}function z(t,n,e){let r=W(),i=gr();if(Yt(r,i,n)){let o=Ve(),a=Aa();Fy(a,r,t,n,r[Ee],e)}return z}function Nf(t,n,e,r,i){Qf(n,t,e,i?"class":"style",r)}function v(t,n,e,r){let i=W(),o=i[L],a=t+ze,s=o.firstCreatePass?l_(a,i,2,n,Vy,Mm(),e,r):o.data[a];if(Kn(s)){let l=i[En].tracingService;if(l&&l.componentCreate){let c=o.data[s.directiveStart+s.componentOffset];return l.componentCreate(r_(c),()=>(Fv(t,n,i,s,r),v))}}return Fv(t,n,i,s,r),v}function Fv(t,n,e,r,i){if(jy(r,e,t,n,E_),Ma(r)){let o=e[L];Zf(o,e,r),by(o,r,e)}i!=null&&Lc(e,r)}function y(){let t=Ve(),n=rt(),e=By(n);return t.firstCreatePass&&c_(t,e),Tm(e)&&Am(),Im(),e.classesWithoutHost!=null&&mS(e)&&Nf(t,e,W(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&fS(e)&&Nf(t,e,W(),e.stylesWithoutHost,!1),y}function Z(t,n,e,r){return v(t,n,e,r),y(),Z}function Ne(t,n,e,r){let i=W(),o=i[L],a=t+ze,s=o.firstCreatePass?hk(a,o,2,n,e,r):o.data[a];return jy(s,i,t,n,E_),r!=null&&Lc(i,s),Ne}function Re(){let t=rt(),n=By(t);return Tm(n)&&Am(),Im(),Re}function Mt(t,n,e,r){return Ne(t,n,e,r),Re(),Mt}var E_=(t,n,e,r,i)=>(oc(!0),vy(n[Ee],r,jm()));function xr(){return W()}function mt(t,n,e){let r=W(),i=gr();if(Yt(r,i,n)){let o=Ve(),a=Aa();Py(a,r,t,n,r[Ee],e)}return mt}var Fa=void 0;function lI(t){let n=Math.floor(Math.abs(t)),e=t.toString().replace(/^[^.]*\.?/,"").length;return n===1&&e===0?1:5}var cI=["en",[["a","p"],["AM","PM"]],[["AM","PM"]],[["S","M","T","W","T","F","S"],["Sun","Mon","Tue","Wed","Thu","Fri","Sat"],["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],["Su","Mo","Tu","We","Th","Fr","Sa"]],Fa,[["J","F","M","A","M","J","J","A","S","O","N","D"],["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],["January","February","March","April","May","June","July","August","September","October","November","December"]],Fa,[["B","A"],["BC","AD"],["Before Christ","Anno Domini"]],0,[6,0],["M/d/yy","MMM d, y","MMMM d, y","EEEE, MMMM d, y"],["h:mm\u202Fa","h:mm:ss\u202Fa","h:mm:ss\u202Fa z","h:mm:ss\u202Fa zzzz"],["{1}, {0}",Fa,Fa,Fa],[".",",",";","%","+","-","E","\xD7","\u2030","\u221E","NaN",":"],["#,##0.###","#,##0%","\xA4#,##0.00","#E0"],"USD","$","US Dollar",{},"ltr",lI],Xm=Object.create(null);function Za(t){let n=dI(t),e=Pv(n);if(e)return e;let r=n.split("-")[0];if(e=Pv(r),e)return e;if(r==="en")return cI;throw new D(701,!1)}function Pv(t){if(!(t in Xm)){let n=Jr.ng&&Jr.ng.common&&Jr.ng.common.locales&&Jr.ng.common.locales[t];return n!==void 0&&(Xm[t]=n),n}return Xm[t]}var gi={LocaleId:0,DayPeriodsFormat:1,DayPeriodsStandalone:2,DaysFormat:3,DaysStandalone:4,MonthsFormat:5,MonthsStandalone:6,Eras:7,FirstDayOfWeek:8,WeekendRange:9,DateFormat:10,TimeFormat:11,DateTimeFormat:12,NumberSymbols:13,NumberFormats:14,CurrencyCode:15,CurrencySymbol:16,CurrencyName:17,Currencies:18,Directionality:19,PluralCase:20,ExtraData:21};function dI(t){return t.toLowerCase().replace(/_/g,"-")}var Qa="en-US",uI="USD";var mI=Qa;function k_(t){typeof t=="string"&&(mI=t.toLowerCase().replace(/_/g,"-"))}function B(t,n,e){let r=W(),i=Ve(),o=rt();return I_(i,r,r[Ee],o,t,n,e),B}function lo(t,n,e){let r=W(),i=Ve(),o=rt();return(o.type&3||e)&&i_(o,i,r,e,r[Ee],t,n,pc(o,r,n)),lo}function I_(t,n,e,r,i,o,a){let s=!0,l=null;if((r.type&3||a)&&(l??=pc(r,n,o),i_(r,t,n,a,e,i,o,l)&&(s=!1)),s){let c=r.outputs?.[i],d=r.hostDirectiveOutputs?.[i];if(d&&d.length)for(let m=0;m<d.length;m+=2){let p=d[m],f=d[m+1];l??=pc(r,n,o),Ev(r,n,p,f,i,l)}if(c&&c.length)for(let m of c)l??=pc(r,n,o),Ev(r,n,m,i,i,l)}}function fe(t=1){return Jb(t)}function fI(t,n){let e=null,r=$S(t);for(let i=0;i<n.length;i++){let o=n[i];if(o==="*"){e=i;continue}if(r===null?Cy(t,o,!0):qS(r,o))return i}return e}function Oe(t){let n=W()[gt][lt];if(!n.projection){let e=t?t.length:1,r=n.projection=Cb(e,null),i=r.slice(),o=n.child;for(;o!==null;){if(o.type!==128){let a=t?fI(o,t):0;a!==null&&(i[a]?i[a].projectionNext=o:r[a]=o,i[a]=o)}o=o.next}}}function ce(t,n=0,e,r,i,o){let a=W(),s=Ve(),l=r?t+1:null;l!==null&&$a(a,s,l,r,i,o,null,e);let c=so(s,ze+t,16,null,e||null);c.projection===null&&(c.projection=n),Fm();let m=!a[ei]||Nm();a[gt][lt].projection[c.projection]===null&&l!==null?pI(a,s,l):m&&!Tc(c)&&fE(s,a,c)}function pI(t,n,e){let r=ze+e,i=n.data[r],o=t[r],a=xc(o,i.tView.ssrId),s=qa(t,i,void 0,{dehydratedView:a});Ya(o,s,0,ro(i,a))}function Ln(t,n,e,r){return g_(t,n,e,r),Ln}function He(t,n,e){return h_(t,n,e),He}function ae(t){let n=W(),e=Ve(),r=tc();Ta(r+1);let i=tp(e,r);if(t.dirty&&Ob(n)===((i.metadata.flags&2)===2)){if(i.matches===null)t.reset([]);else{let o=v_(n,r);t.reset(o,iy),t.notifyOnChanges()}return!0}return!1}function se(){return ep(W(),tc())}function Hc(t,n,e,r,i){return __(n,g_(t,e,r,i)),Hc}function Uc(t,n,e,r){return __(t,h_(n,e,r)),Uc}function $c(t=1){Ta(tc()+t)}function Zt(t){let n=Hb();return wm(n,ze+t)}function cc(t,n){return t<<17|n<<2}function mi(t){return t>>17&32767}function hI(t){return(t&2)==2}function gI(t,n){return t&131071|n<<17}function Tf(t){return t|2}function oo(t){return(t&131068)>>2}function Jm(t,n){return t&-131069|n<<2}function bI(t){return(t&1)===1}function Af(t){return t|1}function vI(t,n,e,r,i,o){let a=o?n.classBindings:n.styleBindings,s=mi(a),l=oo(a);t[r]=e;let c=!1,d;if(Array.isArray(e)){let m=e;d=m[1],(d===null||Ui(m,d)>0)&&(c=!0)}else d=e;if(i)if(l!==0){let p=mi(t[s+1]);t[r+1]=cc(p,s),p!==0&&(t[p+1]=Jm(t[p+1],r)),t[s+1]=gI(t[s+1],r)}else t[r+1]=cc(s,0),s!==0&&(t[s+1]=Jm(t[s+1],r)),s=r;else t[r+1]=cc(l,0),s===0?s=r:t[l+1]=Jm(t[l+1],r),l=r;c&&(t[r+1]=Tf(t[r+1])),Lv(t,d,r,!0),Lv(t,d,r,!1),yI(n,d,t,r,o),a=cc(s,l),o?n.classBindings=a:n.styleBindings=a}function yI(t,n,e,r,i){let o=i?t.residualClasses:t.residualStyles;o!=null&&typeof n=="string"&&Ui(o,n)>=0&&(e[r+1]=Af(e[r+1]))}function Lv(t,n,e,r){let i=t[e+1],o=n===null,a=r?mi(i):oo(i),s=!1;for(;a!==0&&(s===!1||o);){let l=t[a],c=t[a+1];_I(l,n)&&(s=!0,t[a+1]=r?Af(c):Tf(c)),a=r?mi(c):oo(c)}s&&(t[e+1]=r?Tf(i):Af(i))}function _I(t,n){return t===null||n==null||(Array.isArray(t)?t[1]:t)===n?!0:Array.isArray(t)&&typeof n=="string"?Ui(t,n)>=0:!1}var pn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function xI(t){return t.substring(pn.key,pn.keyEnd)}function wI(t){return CI(t),M_(t,N_(t,0,pn.textEnd))}function M_(t,n){let e=pn.textEnd;return e===n?-1:(n=pn.keyEnd=DI(t,pn.key=n,e),N_(t,n,e))}function CI(t){pn.key=0,pn.keyEnd=0,pn.value=0,pn.valueEnd=0,pn.textEnd=t.length}function N_(t,n,e){for(;n<e&&t.charCodeAt(n)<=32;)n++;return n}function DI(t,n,e){for(;n<e&&t.charCodeAt(n)>32;)n++;return n}function bi(t,n,e){return T_(t,n,e,!1),bi}function U(t,n){return T_(t,n,null,!0),U}function Vt(t){EI(AI,SI,t,!0)}function SI(t,n){for(let e=wI(n);e>=0;e=M_(n,e))Gl(t,xI(n),!0)}function T_(t,n,e,r){let i=W(),o=Ve(),a=Jl(2);if(o.firstUpdatePass&&R_(o,t,a,r),n!==It&&Yt(i,a,n)){let s=o.data[Mn()];O_(o,s,i,i[Ee],t,i[a+1]=OI(n,e),r,a)}}function EI(t,n,e,r){let i=Ve(),o=Jl(2);i.firstUpdatePass&&R_(i,null,o,r);let a=W();if(e!==It&&Yt(a,o,e)){let s=i.data[Mn()];if(F_(s,r)&&!A_(i,o)){let l=r?s.classesWithoutHost:s.stylesWithoutHost;l!==null&&(e=jl(l,e||"")),Nf(i,s,a,e,r)}else RI(i,s,a,a[Ee],a[o+1],a[o+1]=TI(t,n,e),r,o)}}function A_(t,n){return n>=t.expandoStartIndex}function R_(t,n,e,r){let i=t.data;if(i[e+1]===null){let o=i[Mn()],a=A_(t,e);F_(o,r)&&n===null&&!a&&(n=!1),n=kI(i,o,n,r),vI(i,o,n,e,a,r)}}function kI(t,n,e,r){let i=Kb(t),o=r?n.residualClasses:n.residualStyles;if(i===null)(r?n.classBindings:n.styleBindings)===0&&(e=ef(null,t,n,e,r),e=Ga(e,n.attrs,r),o=null);else{let a=n.directiveStylingLast;if(a===-1||t[a]!==i)if(e=ef(i,t,n,e,r),o===null){let l=II(t,n,r);l!==void 0&&Array.isArray(l)&&(l=ef(null,t,n,l[1],r),l=Ga(l,n.attrs,r),MI(t,n,r,l))}else o=NI(t,n,r)}return o!==void 0&&(r?n.residualClasses=o:n.residualStyles=o),e}function II(t,n,e){let r=e?n.classBindings:n.styleBindings;if(oo(r)!==0)return t[mi(r)]}function MI(t,n,e,r){let i=e?n.classBindings:n.styleBindings;t[mi(i)]=r}function NI(t,n,e){let r,i=n.directiveEnd;for(let o=1+n.directiveStylingLast;o<i;o++){let a=t[o].hostAttrs;r=Ga(r,a,e)}return Ga(r,n.attrs,e)}function ef(t,n,e,r,i){let o=null,a=e.directiveEnd,s=e.directiveStylingLast;for(s===-1?s=e.directiveStart:s++;s<a&&(o=n[s],r=Ga(r,o.hostAttrs,i),o!==t);)s++;return t!==null&&(e.directiveStylingLast=s),r}function Ga(t,n,e){let r=e?1:2,i=-1;if(n!==null)for(let o=0;o<n.length;o++){let a=n[o];typeof a=="number"?i=a:i===r&&(Array.isArray(t)||(t=t===void 0?[]:["",t]),Gl(t,a,e?!0:n[++o]))}return t===void 0?null:t}function TI(t,n,e){if(e==null||e==="")return ht;let r=[],i=ja(e);if(Array.isArray(i))for(let o=0;o<i.length;o++)t(r,i[o],!0);else if(i instanceof Set)for(let o of i)t(r,o,!0);else if(typeof i=="object")for(let o in i)Object.hasOwn(i,o)&&t(r,o,i[o]);else typeof i=="string"&&n(r,i);return r}function AI(t,n,e){let r=String(n);r!==""&&!r.includes(" ")&&Gl(t,r,e)}function RI(t,n,e,r,i,o,a,s){i===It&&(i=ht);let l=0,c=0,d=0<i.length?i[0]:null,m=0<o.length?o[0]:null;for(;d!==null||m!==null;){let p=l<i.length?i[l+1]:void 0,f=c<o.length?o[c+1]:void 0,h=null,w;d===m?(l+=2,c+=2,p!==f&&(h=m,w=f)):m===null||d!==null&&d<m?(l+=2,h=d):(c+=2,h=m,w=f),h!==null&&O_(t,n,e,r,h,w,a,s),d=l<i.length?i[l]:null,m=c<o.length?o[c]:null}}function O_(t,n,e,r,i,o,a,s){if(!(n.type&3))return;let l=t.data,c=l[s+1],d=bI(c)?Vv(l,n,e,i,oo(c),a):void 0;if(!Nc(d)){Nc(o)||hI(c)&&(o=Vv(l,null,e,i,s,a));let m=xm(Mn(),e);hE(r,a,m,i,o)}}function Vv(t,n,e,r,i,o){let a=n===null,s;for(;i>0;){let l=t[i],c=Array.isArray(l),d=c?l[1]:l,m=d===null,p=e[i+1];p===It&&(p=m?ht:void 0);let f=m?Wl(p,r):d===r?p:void 0;if(c&&!Nc(f)&&(f=Wl(l,r)),Nc(f)&&(s=f,a))return s;let h=t[i+1];i=a?mi(h):oo(h)}if(n!==null){let l=o?n.residualClasses:n.residualStyles;l!=null&&(s=Wl(l,r))}return s}function Nc(t){return t!==void 0}function OI(t,n){return t==null||t===""||(typeof n=="string"?t=ja(t)+n:typeof t=="object"&&(t=_a(ja(t)))),t}function F_(t,n){return(t.flags&(n?8:16))!==0}function M(t,n=""){let e=W(),r=Ve(),i=t+ze,o=r.firstCreatePass?so(r,i,1,n,null):r.data[i],a=FI(r,e,o,n);e[i]=a,ic()&&Gf(r,e,a,o),Zi(o,!1)}var FI=(t,n,e,r)=>(oc(!0),OS(n[Ee],r));function PI(t,n,e,r=""){return Yt(t,gr(),e)?n+Da(e)+r:It}function LI(t,n,e,r,i,o=""){let a=$b(),s=rk(t,a,e,i);return Jl(2),s?n+Da(e)+r+Da(i)+o:It}function Qt(t){return St("",t),Qt}function St(t,n,e){let r=W(),i=PI(r,t,n,e);return i!==It&&P_(r,Mn(),i),St}function vi(t,n,e,r,i){let o=W(),a=LI(o,t,n,e,r,i);return a!==It&&P_(o,Mn(),a),vi}function P_(t,n,e){let r=xm(n,t);FS(t[Ee],r,e)}function co(t,n,e){lc(n)&&(n=n());let r=W(),i=gr();if(Yt(r,i,n)){let o=Ve(),a=Aa();Fy(a,r,t,n,r[Ee],e)}return co}function Xa(t,n){let e=lc(t);return e&&t.set(n),e}function uo(t,n){let e=W(),r=Ve(),i=rt();return I_(r,e,e[Ee],i,t,n),uo}function jv(t,n,e){let r=Ve();r.firstCreatePass&&L_(n,r.data,r.blueprint,In(t),e)}function L_(t,n,e,r,i){if(t=ot(t),Array.isArray(t))for(let o=0;o<t.length;o++)L_(t[o],n,e,r,i);else{let o=Ve(),a=W(),s=rt(),l=Kr(t)?t:ot(t.provide),c=hm(t),d=s.providerIndexes&1048575,m=s.directiveStart,p=s.providerIndexes>>20;if(Kr(t)||!t.multi){let f=new ui(c,i,Ze,null),h=nf(l,n,i?d:d+p,m);h===-1?(af(yc(s,a),o,l),tf(o,t,n.length),n.push(l),s.directiveStart++,s.directiveEnd++,i&&(s.providerIndexes+=1048576),e.push(f),a.push(f)):(e[h]=f,a[h]=f)}else{let f=nf(l,n,d+p,m),h=nf(l,n,d,d+p),w=f>=0&&e[f],I=h>=0&&e[h];if(i&&!I||!i&&!w){af(yc(s,a),o,l);let P=BI(i?jI:VI,e.length,i,r,c,t);!i&&I&&(e[h].providerFactory=P),tf(o,t,n.length,0),n.push(l),s.directiveStart++,s.directiveEnd++,i&&(s.providerIndexes+=1048576),e.push(P),a.push(P)}else{let P=V_(e[i?h:f],c,!i&&r);tf(o,t,f>-1?f:h,P)}!i&&r&&I&&e[h].componentProviders++}}}function tf(t,n,e,r){let i=Kr(n),o=Mb(n);if(i||o){let l=(o?ot(n.useClass):n).prototype.ngOnDestroy;if(l){let c=t.destroyHooks||(t.destroyHooks=[]);if(!i&&n.multi){let d=c.indexOf(e);d===-1?c.push(e,[r,l]):c[d+1].push(r,l)}else c.push(e,l)}}}function V_(t,n,e){return e&&t.componentProviders++,t.multi.push(n)-1}function nf(t,n,e,r){for(let i=e;i<r;i++)if(n[i]===t)return i;return-1}function VI(t,n,e,r,i){return Rf(this.multi,[])}function jI(t,n,e,r,i){let o=this.multi,a;if(this.providerFactory){let s=this.providerFactory.componentProviders,l=Va(r,r[L],this.providerFactory.index,i);a=l.slice(0,s),Rf(o,a);for(let c=s;c<l.length;c++)a.push(l[c])}else a=[],Rf(o,a);return a}function Rf(t,n){for(let e=0;e<t.length;e++){let r=t[e];n.push(r())}return n}function BI(t,n,e,r,i,o){let a=new ui(t,e,Ze,null);return a.multi=[],a.index=n,a.componentProviders=0,V_(a,i,r&&!e),a}function ke(t,n){return e=>{e.providersResolver=(r,i)=>jv(r,i?i(t):t,!1),n&&(e.viewProvidersResolver=(r,i)=>jv(r,i?i(n):n,!0))}}function zI(t,n){let e=t[n];return e===It?void 0:e}function HI(t,n,e,r,i,o){let a=n+e;return Yt(t,a,i)?nk(t,a+1,o?r.call(o,i):r(i)):zI(t,a+1)}function mo(t,n){let e=Ve(),r,i=t+ze;e.firstCreatePass?(r=UI(n,e.pipeRegistry),e.data[i]=r,r.onDestroy&&(e.destroyHooks??=[]).push(i,r.onDestroy)):r=e.data[i];let o=r.factory||(r.factory=dr(r.type,!0)),a,s=Dt(Ze);try{let l=vc(!1),c=o();return vc(l),Cm(e,W(),i,c),c}finally{Dt(s)}}function UI(t,n){if(n)for(let e=n.length-1;e>=0;e--){let r=n[e];if(t===r.name)return r}}function fo(t,n,e){let r=t+ze,i=W(),o=wm(i,r);return $I(i,r)?HI(i,Ub(),n,o.transform,e,o):o.transform(e)}function $I(t,n){return t[L].data[n].pure}function up(t,n){return Vc(t,n)}var j_=(()=>{class t{applicationErrorHandler=u(mn);appRef=u(Kt);taskService=u(vr);ngZone=u(T);zonelessEnabled=u(Oa);tracing=u(Pn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ue;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(va):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(Gm,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:case 2:{this.appRef.dirtyFlags|=2;break}case 3:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let r=this.useMicrotaskScheduler?rv:zm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>r(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>r(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(va+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(r){this.applicationErrorHandler(r)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function B_(){return[{provide:Wn,useExisting:j_},{provide:T,useClass:ya},{provide:Oa,useValue:!0}]}var mp=(()=>{class t{compileModuleSync(e){return new kc(e)}compileModuleAsync(e){return Promise.resolve(this.compileModuleSync(e))}clearCache(){}clearCacheFor(e){}getModuleId(e){}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function GI(){return typeof $localize<"u"&&$localize.locale||Qa}var yi=new b("",{factory:()=>u(yi,{optional:!0,skipSelf:!0})||GI()}),fp=new b("",{factory:()=>uI});function ft(t,n){return oa(t,n?.equal)}function Ie(t){return Fg(t)}var z_=class t extends Error{_brand;constructor(n){super(n)}static IDLE=new t("IDLE");static LOADING=new t("LOADING")};function Y_(t,n){let e=Object.create(Hv);e.value=t,e.transformFn=n?.transform;function r(){if(ar(e),e.value===Of){let i=null;throw new D(-950,i)}return e.value}return r[et]=e,r}var jt=class{attributeName;constructor(n){this.attributeName=n}__NG_ELEMENT_ID__=()=>Pf(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function bp(t){return sM(t)?t.default:t}function sM(t){return t&&typeof t=="object"&&"default"in t}function H_(t,n){return Y_(t,n)}function lM(t){return Y_(Of,t)}var po=(H_.required=lM,H_);function U_(t,n){return np(n)}function cM(t,n){return rp(n)}var es=(U_.required=cM,U_);function $_(t,n){return np(n)}function dM(t,n){return rp(n)}var K_=($_.required=dM,$_);var uM=1e4;var E8=uM-1e3;var ve=(()=>{class t{static __NG_ELEMENT_ID__=mM}return t})();function mM(t){return fM(rt(),W(),(t&16)===16)}function fM(t,n,e){if(Kn(t)&&!e){let r=Wt(t.index,n);return new yr(r,r)}else if(t.type&175){let r=n[gt];return new yr(r,n)}return null}var hp=new b(""),pM=new b("");function Ja(t){return!t.moduleRef}function hM(t){let n=Ja(t)?t.r3Injector:t.moduleRef.injector,e=n.get(T);return e.run(()=>{Ja(t)?t.r3Injector.resolveInjectorInitializers():t.moduleRef.resolveInjectorInitializers();let r=n.get(mn),i;if(e.runOutsideAngular(()=>{i=e.onError.subscribe({next:r})}),Ja(t)){let o=()=>n.destroy(),a=t.platformInjector.get(hp);a.add(o),n.onDestroy(()=>{i.unsubscribe(),a.delete(o)})}else{let o=()=>t.moduleRef.destroy(),a=t.platformInjector.get(hp);a.add(o),t.moduleRef.onDestroy(()=>{La(t.allPlatformModules,t.moduleRef),i.unsubscribe(),a.delete(o)})}return bM(r,e,()=>{let o=n.get(vr),a=o.add(),s=n.get(ap);return s.runInitializers(),s.donePromise.then(()=>{let l=n.get(yi,Qa);if(k_(l||Qa),!n.get(pM,!0))return Ja(t)?n.get(Kt):(t.allPlatformModules.push(t.moduleRef),t.moduleRef);if(Ja(t)){let d=n.get(Kt);return t.rootComponent!==void 0&&d.bootstrap(t.rootComponent),d}else return gM?.(t.moduleRef,t.allPlatformModules),t.moduleRef}).finally(()=>{o.remove(a)})})})}var gM;function bM(t,n,e){try{let r=e();return Xn(r)?r.catch(i=>{throw n.runOutsideAngular(()=>t(i)),i}):r}catch(r){throw n.runOutsideAngular(()=>t(r)),r}}var Gc=null;function vM(t=[],n){return J.create({name:n,providers:[{provide:Ea,useValue:"platform"},{provide:hp,useValue:new Set([()=>Gc=null])},...t]})}function yM(t=[]){if(Gc)return Gc;let n=vM(t);return Gc=n,S_(),_M(n),n}function _M(t){let n=t.get(ac,null);st(t,()=>{n?.forEach(e=>e())})}function Z_(t){let{rootComponent:n,appProviders:e,platformProviders:r,platformRef:i}=t;be(pe.BootstrapApplicationStart);try{let o=i?.injector??yM(r),a=[B_(),av,...e||[]],s=new Ua({providers:a,parent:o,debugName:"",runEnvironmentInitializers:!1});return hM({r3Injector:s.injector,platformInjector:o,rootComponent:n})}catch(o){return Promise.reject(o)}finally{be(pe.BootstrapApplicationEnd)}}function N(t){return typeof t=="boolean"?t:t!=null&&t!=="false"}function er(t,n=NaN){return!isNaN(parseFloat(t))&&!isNaN(Number(t))?Number(t):n}var pp=Symbol("NOT_SET"),Q_=new Set,xM=R(g({},Oi),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:pp,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(t){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==pp&&!Ai(this))return this.signal;try{for(let i of this.cleanup??Q_)i()}finally{this.cleanup?.clear()}let n=[];t!==void 0&&n.push(t),n.push(this.registerCleanupFn);let e=sr(this),r;try{r=this.userFn.apply(null,n)}finally{jr(this,e)}return(this.value===pp||!this.equal(this.value,r))&&(this.value=r,this.version++),this.signal}}),gp=class extends Ba{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(n,e,r,i,o,a=null,s){super(n,[void 0,void 0,void 0,void 0],r,!1,o.get(at),a),this.scheduler=i;for(let l of Hf){let c=e[l];if(c===void 0)continue;let d=Object.create(xM);d.sequence=this,d.phase=l,d.userFn=c,d.dirty=!0,d.signal=()=>(ar(d),d.value),d.signal[et]=d,d.registerCleanupFn=m=>(d.cleanup??=new Set).add(m),this.nodes[l]=d,this.hooks[l]=m=>d.phaseFn(m)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();super.destroy();for(let n of this.nodes)if(n)try{for(let e of n.cleanup??Q_)e()}finally{lr(n)}}};function vp(t,n){let e=n?.injector??u(J),r=e.get(Wn),i=e.get(Rc),o=e.get(Pn,null,{optional:!0});i.impl??=e.get(Uf);let a=t;typeof a=="function"&&(a={mixedReadWrite:t});let s=e.get(Qi,null,{optional:!0}),l=new gp(i.impl,[a.earlyRead,a.write,a.mixedReadWrite,a.read],s?.view,r,e,o?.snapshot(null),void 0);return i.impl.register(l),l}function Wc(t,n){let e=ur(t),r=n.elementInjector||Gi(),o=new io(e).create(r,n.projectableNodes,n.hostElement,n.environmentInjector,n.directives,n.bindings);return n.onError&&(o.hostView._lView[Yi]=n.onError),o}var X_=null;function Xt(){return X_}function yp(t){X_??=t}var ts=class{},qc=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(r){return new(r||t)};static \u0275prov=me({token:t,factory:()=>u(J_),providedIn:"platform"})}return t})();var J_=(()=>{class t extends qc{_location;_history;_doc=u(G);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Xt().getBaseHref(this._doc)}onPopState(e){let r=Xt().getGlobalEventTarget(this._doc,"window");return r.addEventListener("popstate",e,!1),()=>r.removeEventListener("popstate",e)}onHashChange(e){let r=Xt().getGlobalEventTarget(this._doc,"window");return r.addEventListener("hashchange",e,!1),()=>r.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,r,i){this._history.pushState(e,r,i)}replaceState(e,r,i){this._history.replaceState(e,r,i)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(r){return new(r||t)};static \u0275prov=me({token:t,factory:()=>new t,providedIn:"platform"})}return t})();function nx(t,n){return t?n?t.endsWith("/")?n.startsWith("/")?t+n.slice(1):t+n:n.startsWith("/")?t+n:`${t}/${n}`:t:n}function ex(t){let n=t.search(/#|\?|$/);return t[n-1]==="/"?t.slice(0,n-1)+t.slice(n):t}function Cr(t){return t&&t[0]!=="?"?`?${t}`:t}var Yc=(()=>{class t{historyGo(e){throw new Error("")}static \u0275fac=function(r){return new(r||t)};static \u0275prov=me({token:t,factory:()=>u(CM),providedIn:"root"})}return t})(),wM=new b(""),CM=(()=>{class t extends Yc{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,r){super(),this._platformLocation=e,this._baseHref=r??this._platformLocation.getBaseHrefFromDOM()??u(G).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return nx(this._baseHref,e)}path(e=!1){let r=this._platformLocation.pathname+Cr(this._platformLocation.search),i=this._platformLocation.hash;return i&&e?`${r}${i}`:r}pushState(e,r,i,o){let a=this.prepareExternalUrl(i+Cr(o));this._platformLocation.pushState(e,r,a)}replaceState(e,r,i,o){let a=this.prepareExternalUrl(i+Cr(o));this._platformLocation.replaceState(e,r,a)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(r){return new(r||t)(X(qc),X(wM,8))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var Dr=(()=>{class t{_subject=new E;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let r=this._locationStrategy.getBaseHref();this._basePath=EM(ex(tx(r))),this._locationStrategy.onPopState(i=>{let o={url:this.path(!0),pop:!0,state:i.state,type:i.type};i.hasUAVisualTransition&&(o.hasUAVisualTransition=!0),this._subject.next(o)})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,r=""){return this.path()==this.normalize(e+Cr(r))}normalize(e){return t.stripTrailingSlash(SM(this._basePath,tx(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,r="",i=null){this._locationStrategy.pushState(i,"",e,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Cr(r)),i)}replaceState(e,r="",i=null){this._locationStrategy.replaceState(i,"",e,r),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+Cr(r)),i)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(r=>{this._notifyUrlChangeListeners(r.url,r.state)}),()=>{let r=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(r,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",r){this._urlChangeListeners.forEach(i=>i(e,r))}subscribe(e,r,i){return this._subject.subscribe({next:e,error:r??void 0,complete:i??void 0})}static normalizeQueryParams=Cr;static joinWithSlash=nx;static stripTrailingSlash=ex;static \u0275fac=function(r){return new(r||t)(X(Yc))};static \u0275prov=me({token:t,factory:()=>DM(),providedIn:"root"})}return t})();function DM(){return new Dr(X(Yc))}function SM(t,n){if(!t||!n.startsWith(t))return n;let e=n.substring(t.length);return e===""||["/",";","?","#"].includes(e[0])?e:n}function tx(t){return t.replace(/\/index\.html$/,"")}function EM(t){if(new RegExp("^(https?:)?//").test(t)){let[,e]=t.split(/\/\/[^\/]+/);return e}return t}var ox={ADP:[void 0,void 0,0],AFN:[void 0,"\u060B",0],ALL:[void 0,void 0,0],AMD:[void 0,"\u058F",2],AOA:[void 0,"Kz"],ARS:[void 0,"$"],AUD:["A$","$"],AZN:[void 0,"\u20BC"],BAM:[void 0,"KM"],BBD:[void 0,"$"],BDT:[void 0,"\u09F3"],BHD:[void 0,void 0,3],BIF:[void 0,void 0,0],BMD:[void 0,"$"],BND:[void 0,"$"],BOB:[void 0,"Bs"],BRL:["R$"],BSD:[void 0,"$"],BWP:[void 0,"P"],BYN:[void 0,void 0,2],BYR:[void 0,void 0,0],BZD:[void 0,"$"],CAD:["CA$","$",2],CHF:[void 0,void 0,2],CLF:[void 0,void 0,4],CLP:[void 0,"$",0],CNY:["CN\xA5","\xA5"],COP:[void 0,"$",2],CRC:[void 0,"\u20A1",2],CUC:[void 0,"$"],CUP:[void 0,"$"],CZK:[void 0,"K\u010D",2],DJF:[void 0,void 0,0],DKK:[void 0,"kr",2],DOP:[void 0,"$"],EGP:[void 0,"E\xA3"],ESP:[void 0,"\u20A7",0],EUR:["\u20AC"],FJD:[void 0,"$"],FKP:[void 0,"\xA3"],GBP:["\xA3"],GEL:[void 0,"\u20BE"],GHS:[void 0,"GH\u20B5"],GIP:[void 0,"\xA3"],GNF:[void 0,"FG",0],GTQ:[void 0,"Q"],GYD:[void 0,"$",2],HKD:["HK$","$"],HNL:[void 0,"L"],HRK:[void 0,"kn"],HUF:[void 0,"Ft",2],IDR:[void 0,"Rp",2],ILS:["\u20AA"],INR:["\u20B9"],IQD:[void 0,void 0,0],IRR:[void 0,void 0,0],ISK:[void 0,"kr",0],ITL:[void 0,void 0,0],JMD:[void 0,"$"],JOD:[void 0,void 0,3],JPY:["\xA5",void 0,0],KGS:[void 0,"\u20C0"],KHR:[void 0,"\u17DB"],KMF:[void 0,"CF",0],KPW:[void 0,"\u20A9",0],KRW:["\u20A9",void 0,0],KWD:[void 0,void 0,3],KYD:[void 0,"$"],KZT:[void 0,"\u20B8"],LAK:[void 0,"\u20AD",0],LBP:[void 0,"L\xA3",0],LKR:[void 0,"Rs"],LRD:[void 0,"$"],LTL:[void 0,"Lt"],LUF:[void 0,void 0,0],LVL:[void 0,"Ls"],LYD:[void 0,void 0,3],MGA:[void 0,"Ar",0],MGF:[void 0,void 0,0],MMK:[void 0,"K",0],MNT:[void 0,"\u20AE",2],MRO:[void 0,void 0,0],MUR:[void 0,"Rs",2],MXN:["MX$","$"],MYR:[void 0,"RM"],NAD:[void 0,"$"],NGN:[void 0,"\u20A6"],NIO:[void 0,"C$"],NOK:[void 0,"kr",2],NPR:[void 0,"Rs"],NZD:["NZ$","$"],OMR:[void 0,void 0,3],PHP:["\u20B1"],PKR:[void 0,"Rs",2],PLN:[void 0,"z\u0142"],PYG:[void 0,"\u20B2",0],RON:[void 0,"lei"],RSD:[void 0,void 0,0],RUB:[void 0,"\u20BD"],RWF:[void 0,"RF",0],SBD:[void 0,"$"],SEK:[void 0,"kr",2],SGD:[void 0,"$"],SHP:[void 0,"\xA3"],SLE:[void 0,void 0,2],SLL:[void 0,void 0,0],SOS:[void 0,void 0,0],SRD:[void 0,"$"],SSP:[void 0,"\xA3"],STD:[void 0,void 0,0],STN:[void 0,"Db"],SYP:[void 0,"\xA3",0],THB:[void 0,"\u0E3F"],TMM:[void 0,void 0,0],TND:[void 0,void 0,3],TOP:[void 0,"T$"],TRL:[void 0,void 0,0],TRY:[void 0,"\u20BA"],TTD:[void 0,"$"],TWD:["NT$","$",2],TZS:[void 0,void 0,2],UAH:[void 0,"\u20B4"],UGX:[void 0,void 0,0],USD:["$"],UYI:[void 0,void 0,0],UYU:[void 0,"$"],UYW:[void 0,void 0,4],UZS:[void 0,void 0,2],VEF:[void 0,"Bs",2],VND:["\u20AB",void 0,0],VUV:[void 0,void 0,0],XAF:["FCFA",void 0,0],XCD:["EC$","$"],XCG:["Cg."],XOF:["F\u202FCFA",void 0,0],XPF:["CFPF",void 0,0],XXX:["\xA4"],YER:[void 0,void 0,0],ZAR:[void 0,"R"],ZMK:[void 0,void 0,0],ZMW:[void 0,"ZK"],ZWD:[void 0,void 0,0]},wp=(function(t){return t[t.Decimal=0]="Decimal",t[t.Percent=1]="Percent",t[t.Currency=2]="Currency",t[t.Scientific=3]="Scientific",t})(wp||{});var bn={Decimal:0,Group:1,List:2,PercentSign:3,PlusSign:4,MinusSign:5,Exponential:6,SuperscriptingExponent:7,PerMille:8,Infinity:9,NaN:10,TimeSeparator:11,CurrencyDecimal:12,CurrencyGroup:13};function ho(t,n){let e=Za(t),r=e[gi.NumberSymbols][n];if(typeof r>"u"){if(n===bn.CurrencyDecimal)return e[gi.NumberSymbols][bn.Decimal];if(n===bn.CurrencyGroup)return e[gi.NumberSymbols][bn.Group]}return r}function ax(t,n){return Za(t)[gi.NumberFormats][n]}function IM(t){return Za(t)[gi.Currencies]}function sx(t,n,e="en"){let r=IM(e)[t]||ox[t]||[],i=r[1];return n==="narrow"&&typeof i=="string"?i:r[0]||t}var MM=2;function lx(t){let n,e=ox[t];return e&&(n=e[2]),typeof n=="number"?n:MM}var NM=/^(\d+)?\.((\d+)(-(\d+))?)?$/,rx=22,Kc=".",ns="0",TM=";",AM=",",_p="#",ix="\xA4";function RM(t,n,e,r,i,o,a=!1){let s="",l=!1;if(!isFinite(t))s=ho(e,Number.isNaN(t)?bn.NaN:bn.Infinity);else{let c=PM(t);a&&(c=FM(c));let d=n.minInt,m=n.minFrac,p=n.maxFrac;if(o){let $=o.match(NM);if($===null)throw new D(2306,!1);let Je=$[1],na=$[3],$s=$[5];Je!=null&&(d=xp(Je)),na!=null&&(m=xp(na)),$s!=null?p=xp($s):na!=null&&m>p&&(p=m);let ra=100;if(d>ra||m>ra||p>ra)throw new D(2306,!1)}LM(c,m,p);let f=c.digits,h=c.integerLen,w=c.exponent,I=[];for(l=f.every($=>!$);h<d;h++)f.unshift(0);for(;h<0;h++)f.unshift(0);h>0?I=f.splice(h,f.length):(I=f,f=[0]);let P=[];for(f.length>=n.lgSize&&P.unshift(f.splice(-n.lgSize,f.length).join(""));f.length>n.gSize;)P.unshift(f.splice(-n.gSize,f.length).join(""));f.length&&P.unshift(f.join("")),s=P.join(ho(e,r)),I.length&&(s+=ho(e,i)+I.join("")),w&&(s+=ho(e,bn.Exponential)+"+"+w)}return t<0&&!l?s=n.negPre+s+n.negSuf:s=n.posPre+s+n.posSuf,s}function cx(t,n,e,r,i){let o=ax(n,wp.Currency),a=OM(o,ho(n,bn.MinusSign));return a.minFrac=lx(r),a.maxFrac=a.minFrac,RM(t,a,n,bn.CurrencyGroup,bn.CurrencyDecimal,i).replace(ix,e).replace(ix,"").trim()}function OM(t,n="-"){let e={minInt:1,minFrac:0,maxFrac:0,posPre:"",posSuf:"",negPre:"",negSuf:"",gSize:0,lgSize:0},r=t.split(TM),i=r[0],o=r[1],a=i.indexOf(Kc)!==-1?i.split(Kc):[i.substring(0,i.lastIndexOf(ns)+1),i.substring(i.lastIndexOf(ns)+1)],s=a[0],l=a[1]||"";e.posPre=s.substring(0,s.indexOf(_p));for(let d=0;d<l.length;d++){let m=l.charAt(d);m===ns?e.minFrac=e.maxFrac=d+1:m===_p?e.maxFrac=d+1:e.posSuf+=m}let c=s.split(AM);if(e.gSize=c[1]?c[1].length:0,e.lgSize=c[2]||c[1]?(c[2]||c[1]).length:0,o){let d=i.length-e.posPre.length-e.posSuf.length,m=o.indexOf(_p);e.negPre=o.substring(0,m).replace(/'/g,""),e.negSuf=o.slice(m+d).replace(/'/g,"")}else e.negPre=n+e.posPre,e.negSuf=e.posSuf;return e}function FM(t){if(t.digits[0]===0)return t;let n=t.digits.length-t.integerLen;return t.exponent?t.exponent+=2:(n===0?t.digits.push(0,0):n===1&&t.digits.push(0),t.integerLen+=2),t}function PM(t){let n=Math.abs(t)+"",e=0,r,i,o,a,s;for((i=n.indexOf(Kc))>-1&&(n=n.replace(Kc,"")),(o=n.search(/e/i))>0?(i<0&&(i=o),i+=+n.slice(o+1),n=n.substring(0,o)):i<0&&(i=n.length),o=0;n.charAt(o)===ns;o++);if(o===(s=n.length))r=[0],i=1;else{for(s--;n.charAt(s)===ns;)s--;for(i-=o,r=[],a=0;o<=s;o++,a++)r[a]=Number(n.charAt(o))}return i>rx&&(r=r.splice(0,rx-1),e=i-1,i=1),{digits:r,exponent:e,integerLen:i}}function LM(t,n,e){if(n>e)throw new D(2307,!1);let r=t.digits,i=r.length-t.integerLen,o=Math.min(Math.max(n,i),e),a=o+t.integerLen,s=r[a];if(a>0){r.splice(Math.max(t.integerLen,a));for(let m=a;m<r.length;m++)r[m]=0}else{i=Math.max(0,i),t.integerLen=1,r.length=Math.max(1,a=o+1),r[0]=0;for(let m=1;m<a;m++)r[m]=0}if(s>=5)if(a-1<0){for(let m=0;m>a;m--)r.unshift(0),t.integerLen++;r.unshift(1),t.integerLen++}else r[a-1]++;for(;i<Math.max(0,o);i++)r.push(0);let l=o!==0,c=n+t.integerLen,d=r.reduceRight(function(m,p,f,h){return p=p+m,h[f]=p<10?p:p-10,l&&(h[f]===0&&f>=c?h.pop():l=!1),p>=10?1:0},0);d&&(r.unshift(d),t.integerLen++)}function xp(t){let n=parseInt(t);if(isNaN(n))throw new D(2305,!1);return n}var Cp=(()=>{class t{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u(J);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let r=this._viewContainerRef;if(this._viewRef&&r.remove(r.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let i=this._createContextForwardProxy();this._viewRef=r.createEmbeddedView(this.ngTemplateOutlet,i,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,r,i)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,r,i):!1,get:(e,r,i)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,r,i)}})}static \u0275fac=function(r){return new(r||t)(Ze(bt))};static \u0275dir=j({type:t,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Ae]})}return t})();function VM(t,n){return new D(2100,!1)}var rs=(()=>{class t{_locale;_defaultCurrencyCode;constructor(e,r="USD"){this._locale=e,this._defaultCurrencyCode=r}transform(e,r=this._defaultCurrencyCode,i="symbol",o,a){if(!jM(e))return null;a||=this._locale,typeof i=="boolean"&&(i=i?"symbol":"code");let s=r||this._defaultCurrencyCode;i!=="code"&&(i==="symbol"||i==="symbol-narrow"?s=sx(s,i==="symbol"?"wide":"narrow",a):s=i);try{let l=BM(e);return cx(l,a,s,r,o)}catch(l){throw VM(t,l.message)}}static \u0275fac=function(r){return new(r||t)(Ze(yi,16),Ze(fp,16))};static \u0275pipe=op({name:"currency",type:t,pure:!0})}return t})();function jM(t){return!(t==null||t===""||t!==t)}function BM(t){if(typeof t=="string"&&!isNaN(Number(t)-parseFloat(t)))return Number(t);if(typeof t!="number")throw new D(2309,!1);return t}function Dp(t,n){n=encodeURIComponent(n);for(let e of t.split(";")){let r=e.indexOf("="),[i,o]=r==-1?[e,""]:[e.slice(0,r),e.slice(r+1)];if(i.trim()!==n)continue;let a=o;try{a=decodeURIComponent(o)}catch{}return a.length>1&&a[0]==='"'&&a[a.length-1]==='"'&&(a=a.slice(1,-1)),a}return null}var Sp="browser";function dx(t){return t===Sp}var is=class{_doc;constructor(n){this._doc=n}manager},Zc=(()=>{class t extends is{constructor(e){super(e)}supports(e){return!0}addEventListener(e,r,i,o){return e.addEventListener(r,i,o),()=>this.removeEventListener(e,r,i,o)}removeEventListener(e,r,i,o){return e.removeEventListener(r,i,o)}static \u0275fac=function(r){return new(r||t)(X(G))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),Jc=new b(""),Mp=(()=>{class t{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,r){this._zone=r,e.forEach(a=>{a.manager=this});let i=e.filter(a=>!(a instanceof Zc));this._plugins=i.slice().reverse();let o=e.find(a=>a instanceof Zc);o&&this._plugins.push(o)}addEventListener(e,r,i,o){return this._findPluginFor(r).addEventListener(e,r,i,o)}getZone(){return this._zone}_findPluginFor(e){let r=this._eventNameToPlugin.get(e);if(r)return r;if(r=this._plugins.find(o=>o.supports(e)),!r)throw new D(-5101,!1);return this._eventNameToPlugin.set(e,r),r}static \u0275fac=function(r){return new(r||t)(X(Jc),X(T))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),Ep="ng-app-id";function ux(t){for(let n of t)n.remove()}function mx(t,n){let e=n.createElement("style");return e.textContent=t,e}function UM(t,n,e,r){let i=t.head?.querySelectorAll(`style[${Ep}="${n}"],link[${Ep}="${n}"]`);if(!i||i.length===0)return!1;for(let o of i)o.removeAttribute(Ep),o instanceof HTMLLinkElement?r.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]});return!0}function Ip(t,n){let e=n.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",t),e}var Np=(()=>{class t{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,r,i,o={}){this.doc=e,this.appId=r,this.nonce=i,UM(e,r,this.inline,this.external)&&this.hosts.add(e.head)}addStyles(e,r){for(let i of e)this.addUsage(i,this.inline,mx);r?.forEach(i=>this.addUsage(i,this.external,Ip))}removeStyles(e,r){for(let i of e)this.removeUsage(i,this.inline);r?.forEach(i=>this.removeUsage(i,this.external))}addUsage(e,r,i){let o=r.get(e);o?o.usage++:r.set(e,{usage:1,elements:[...this.hosts].map(a=>this.addElement(a,i(e,this.doc)))})}removeUsage(e,r){let i=r.get(e);i&&(i.usage--,i.usage<=0&&(ux(i.elements),r.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])ux(e);this.hosts.clear()}addHost(e){if(!this.hosts.has(e)){this.hosts.add(e);for(let[r,{elements:i}]of this.inline)i.push(this.addElement(e,mx(r,this.doc)));for(let[r,{elements:i}]of this.external)i.push(this.addElement(e,Ip(r,this.doc)))}}removeHost(e){this.hosts.delete(e);for(let r of[...this.inline.values(),...this.external.values()]){let i=[];for(let o of r.elements)o.parentNode===e?o.remove():i.push(o);r.elements=i}}addElement(e,r){return this.nonce&&r.setAttribute("nonce",this.nonce),e.appendChild(r)}static \u0275fac=function(r){return new(r||t)(X(G),X(Zn),X(ci,8),X(li))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),kp={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},Tp=/%COMP%/g;var px="%COMP%",$M=`_nghost-${px}`,GM=`_ngcontent-${px}`,WM=!0,qM=new b("",{factory:()=>WM}),YM=new b("");function KM(t){return GM.replace(Tp,t)}function ZM(t){return $M.replace(Tp,t)}function hx(t,n){return n.map(e=>e.replace(Tp,t))}var Ap=(()=>{class t{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;cssVarNamespace;constructor(e,r,i,o,a,s,l=null,c=null,d=null){this.eventManager=e,this.sharedStylesHost=r,this.appId=i,this.removeStylesOnCompDestroy=o,this.doc=a,this.ngZone=s,this.nonce=l,this.tracingService=c,this.cssVarNamespace=d??"",this.defaultRenderer=new os(e,a,s,this.tracingService,this.cssVarNamespace)}createRenderer(e,r){if(!e||!r)return this.defaultRenderer;let i=this.getOrCreateRenderer(e,r);return i instanceof Xc?i.applyToHost(e):i instanceof as&&i.applyStyles(),i}getOrCreateRenderer(e,r){let i=this.rendererByCompId,o=i.get(r.id);if(!o){let a=this.doc,s=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,m=this.tracingService;switch(r.encapsulation){case hn.Emulated:o=new Xc(l,c,r,this.appId,d,a,s,m,this.cssVarNamespace);break;case hn.ShadowDom:return new Qc(l,e,r,a,s,this.nonce,m,this.cssVarNamespace,c);case hn.ExperimentalIsolatedShadowDom:return new Qc(l,e,r,a,s,this.nonce,m,this.cssVarNamespace);default:o=new as(l,c,r,d,a,s,m,this.cssVarNamespace);break}i.set(r.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(r){return new(r||t)(X(Mp),X(fi),X(Zn),X(qM),X(G),X(T),X(ci),X(Pn,8),X(YM,8))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),os=class{eventManager;doc;ngZone;tracingService;cssVarNamespace;data=Object.create(null);throwOnSyntheticProps=!0;constructor(n,e,r,i,o=""){this.eventManager=n,this.doc=e,this.ngZone=r,this.tracingService=i,this.cssVarNamespace=o}destroy(){}destroyNode=null;createElement(n,e){return e?this.doc.createElementNS(kp[e]||e,n):this.doc.createElement(n)}createComment(n){return this.doc.createComment(n)}createText(n){return this.doc.createTextNode(n)}appendChild(n,e){(fx(n)?n.content:n).appendChild(e)}insertBefore(n,e,r){if(n){let i=fx(n)?n.content:n;if(r!=null&&r.parentNode!==i)throw new D(-5106,!1);i.insertBefore(e,r)}}removeChild(n,e){e.remove()}selectRootElement(n,e){let r=typeof n=="string"?this.doc.querySelector(n):n;if(!r)throw new D(-5104,!1);return e||(r.textContent=""),r}parentNode(n){return n.parentNode}nextSibling(n){return n.nextSibling}setAttribute(n,e,r,i){if(i){e=i+":"+e;let o=kp[i];o?n.setAttributeNS(o,e,r):n.setAttribute(e,r)}else n.setAttribute(e,r)}removeAttribute(n,e,r){if(r){let i=kp[r];i?n.removeAttributeNS(i,e):n.removeAttribute(`${r}:${e}`)}else n.removeAttribute(e)}addClass(n,e){n.classList.add(e)}removeClass(n,e){n.classList.remove(e)}setStyle(n,e,r,i){let o=e.startsWith("--");o&&(e=e.replace("%NS%",this.cssVarNamespace)),o||i&(Rn.DashCase|Rn.Important)?n.style.setProperty(e,r,i&Rn.Important?"important":""):n.style[e]=r}removeStyle(n,e,r){let i=e.startsWith("--");i&&(e=e.replace("%NS%",this.cssVarNamespace)),i||r&Rn.DashCase?n.style.removeProperty(e):n.style[e]=""}setProperty(n,e,r){n!=null&&(n[e]=r)}setValue(n,e){n.nodeValue=e}listen(n,e,r,i){if(typeof n=="string"&&(n=Xt().getGlobalEventTarget(this.doc,n),!n))throw new D(-5102,!1);let o=this.decoratePreventDefault(r);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(n,e,o)),this.eventManager.addEventListener(n,e,o,i)}decoratePreventDefault(n){return e=>{if(e==="__ngUnwrap__")return n;n(e)===!1&&e.preventDefault()}}};function fx(t){return t.tagName==="TEMPLATE"&&t.content!==void 0}var Qc=class extends os{hostEl;sharedStylesHost;shadowRoot;constructor(n,e,r,i,o,a,s,l,c){super(n,i,o,s,l),this.hostEl=e,this.sharedStylesHost=c,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let d=r.styles;d=hx(r.id,d).map(p=>p.replace(/%NS%/g,l));for(let p of d){let f=document.createElement("style");a&&f.setAttribute("nonce",a),f.textContent=p,this.shadowRoot.appendChild(f)}let m=r.getExternalStyles?.();if(m)for(let p of m){let f=Ip(p,i);a&&f.setAttribute("nonce",a),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(n){return n===this.hostEl?this.shadowRoot:n}appendChild(n,e){return super.appendChild(this.nodeOrShadowRoot(n),e)}insertBefore(n,e,r){return super.insertBefore(this.nodeOrShadowRoot(n),e,r)}removeChild(n,e){return super.removeChild(null,e)}parentNode(n){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(n)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},as=class extends os{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(n,e,r,i,o,a,s,l,c){super(n,o,a,s,l),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=i;let d=r.styles,m=c?hx(c,d):d;this.styles=m.map(p=>p.replace(/%NS%/g,l)),this.styleUrls=r.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&zf.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Xc=class extends as{contentAttr;hostAttr;constructor(n,e,r,i,o,a,s,l,c){let d=i+"-"+r.id;super(n,e,r,o,a,s,l,c,d),this.contentAttr=KM(d),this.hostAttr=ZM(d)}applyToHost(n){this.applyStyles(),this.setAttribute(n,this.hostAttr,"")}createElement(n,e){let r=super.createElement(n,e);return super.setAttribute(r,this.contentAttr,""),r}};var ed=class t extends ts{supportsDOMEvents=!0;static makeCurrent(){yp(new t)}onAndCancel(n,e,r,i){return n.addEventListener(e,r,i),()=>{n.removeEventListener(e,r,i)}}dispatchEvent(n,e){n.dispatchEvent(e)}remove(n){n.remove()}createElement(n,e){return e=e||this.getDefaultDocument(),e.createElement(n)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(n){return n.nodeType===Node.ELEMENT_NODE}isShadowRoot(n){return n instanceof DocumentFragment}getGlobalEventTarget(n,e){return e==="window"?window:e==="document"?n:e==="body"?n.body:null}getBaseHref(n){let e=QM();return e==null?null:XM(e)}resetBaseElement(){ss=null}getUserAgent(){return window.navigator.userAgent}getCookie(n){return Dp(document.cookie,n)}},ss=null;function QM(){return ss=ss||document.head.querySelector("base"),ss?ss.getAttribute("href"):null}function XM(t){return new URL(t,document.baseURI).pathname}var gx=["alt","control","meta","shift"],JM={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},eN={alt:t=>t.altKey,control:t=>t.ctrlKey,meta:t=>t.metaKey,shift:t=>t.shiftKey},bx=(()=>{class t extends is{constructor(e){super(e)}supports(e){return t.parseEventName(e)!=null}addEventListener(e,r,i,o){let a=t.parseEventName(r),s=t.eventCallback(a.fullKey,i,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Xt().onAndCancel(e,a.domEventName,s,o))}static parseEventName(e){let r=e.toLowerCase().split("."),i=r.shift();if(r.length===0||!(i==="keydown"||i==="keyup"))return null;let o=t._normalizeKey(r.pop()),a="",s=r.indexOf("code");if(s>-1&&(r.splice(s,1),a="code."),gx.forEach(c=>{let d=r.indexOf(c);d>-1&&(r.splice(d,1),a+=c+".")}),a+=o,r.length!=0||o.length===0)return null;let l={};return l.domEventName=i,l.fullKey=a,l}static matchEventFullKeyCode(e,r){let i=JM[e.key]||e.key,o="";return r.indexOf("code.")>-1&&(i=e.code,o="code."),i==null||!i?!1:(i=i.toLowerCase(),i===" "?i="space":i==="."&&(i="dot"),gx.forEach(a=>{if(a!==i){let s=eN[a];s(e)&&(o+=a+".")}}),o+=i,o===r)}static eventCallback(e,r,i){return o=>{t.matchEventFullKeyCode(o,e)&&i.runGuarded(()=>r(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(r){return new(r||t)(X(G))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})();async function Rp(t,n,e){let r=g({rootComponent:t},tN(n,e));return Z_(r)}function tN(t,n){return{platformRef:n?.platformRef,appProviders:[...aN,...t?.providers??[]],platformProviders:oN}}function nN(){ed.makeCurrent()}function rN(){return new Sn}function iN(){return Vf(document),document}var oN=[{provide:li,useValue:Sp},{provide:ac,useValue:nN,multi:!0},{provide:G,useFactory:iN}];var aN=[{provide:Ea,useValue:"root"},{provide:Sn,useFactory:rN},{provide:Jc,useClass:Zc,multi:!0},{provide:Jc,useClass:bx,multi:!0},Ap,{provide:fi,useClass:Np},{provide:Np,useExisting:fi},Mp,{provide:Xe,useExisting:Ap},[]];var vx=(()=>{class t{_doc;constructor(e){this._doc=e}getTitle(){return this._doc.title}setTitle(e){this._doc.title=e||""}static \u0275fac=function(r){return new(r||t)(X(G))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})();var ee="primary",ys=Symbol("RouteTitle"),jp=class{params;constructor(n){this.params=n||{}}has(n){return Object.hasOwn(this.params,n)}get(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e[0]:e}return null}getAll(n){if(this.has(n)){let e=this.params[n];return Array.isArray(e)?e:[e]}return[]}get keys(){return Object.keys(this.params)}};function yo(t){return new jp(t)}function Op(t,n,e){for(let r=0;r<t.length;r++){let i=t[r],o=n[r];if(i[0]===":")e[i.substring(1)]=o;else if(i!==o.path)return!1}return!0}function cN(t,n,e){let r=e.path.split("/"),i=r.indexOf("**");if(i===-1){if(r.length>t.length||e.pathMatch==="full"&&(n.hasChildren()||r.length<t.length))return null;let l={},c=t.slice(0,r.length);return Op(r,c,l)?{consumed:c,posParams:l}:null}if(i!==r.lastIndexOf("**"))return null;let o=r.slice(0,i),a=r.slice(i+1);if(o.length+a.length>t.length||e.pathMatch==="full"&&n.hasChildren()&&e.path!=="**")return null;let s={};return!Op(o,t.slice(0,o.length),s)||!Op(a,t.slice(t.length-a.length),s)?null:{consumed:t,posParams:s}}function ad(t){return new Promise((n,e)=>{t.pipe(Hn()).subscribe({next:r=>n(r),error:r=>e(r)})})}function dN(t,n){if(t.length!==n.length)return!1;for(let e=0;e<t.length;++e)if(!jn(t[e],n[e]))return!1;return!0}function jn(t,n){let e=t?Bp(t):void 0,r=n?Bp(n):void 0;if(!e||!r||e.length!=r.length)return!1;let i;for(let o=0;o<e.length;o++)if(i=e[o],!kx(t[i],n[i]))return!1;return!0}function Bp(t){return[...Object.keys(t),...Object.getOwnPropertySymbols(t)]}function kx(t,n){if(Array.isArray(t)&&Array.isArray(n)){if(t.length!==n.length)return!1;let e=[...t].sort(),r=[...n].sort();return e.every((i,o)=>r[o]===i)}else return t===n}function uN(t){return t.length>0?t[t.length-1]:null}function Di(t){return da(t)?t:Xn(t)?Pe(Promise.resolve(t)):Y(t)}function Ix(t){return da(t)?ad(t):Promise.resolve(t)}var mN={exact:Tx,subset:Ax},Mx={exact:fN,subset:pN,ignored:()=>!0},Nx={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},sd={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function yx(t,n,e){let r=g(g({},sd),e||{});return mN[r.paths](t.root,n.root,r.matrixParams)&&Mx[r.queryParams](t.queryParams,n.queryParams)&&!(r.fragment==="exact"&&t.fragment!==n.fragment)}function fN(t,n){return jn(t,n)}function Tx(t,n,e){if(!xi(t.segments,n.segments)||!rd(t.segments,n.segments,e)||t.numberOfChildren!==n.numberOfChildren)return!1;for(let r in n.children)if(!t.children[r]||!Tx(t.children[r],n.children[r],e))return!1;return!0}function pN(t,n){return Object.keys(n).length<=Object.keys(t).length&&Object.keys(n).every(e=>kx(t[e],n[e]))}function Ax(t,n,e){return Rx(t,n,n.segments,e)}function Rx(t,n,e,r){if(t.segments.length>e.length){let i=t.segments.slice(0,e.length);return!(!xi(i,e)||n.hasChildren()||!rd(i,e,r))}else if(t.segments.length===e.length){if(!xi(t.segments,e)||!rd(t.segments,e,r))return!1;for(let i in n.children)if(!t.children[i]||!Ax(t.children[i],n.children[i],r))return!1;return!0}else{let i=e.slice(0,t.segments.length),o=e.slice(t.segments.length);return!xi(t.segments,i)||!rd(t.segments,i,r)||!t.children[ee]?!1:Rx(t.children[ee],n,o,r)}}function rd(t,n,e){return n.every((r,i)=>Mx[e](t[i].parameters,r.parameters))}var _n=class{root;queryParams;fragment;_queryParamMap;constructor(n=new ye([],{}),e={},r=null){this.root=n,this.queryParams=e,this.fragment=r}get queryParamMap(){return this._queryParamMap??=yo(this.queryParams),this._queryParamMap}toString(){return vN.serialize(this)}},ye=class{segments;children;parent=null;constructor(n,e){this.segments=n,this.children=e,Object.values(e).forEach(r=>r.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return id(this)}},_i=class{path;parameters;_parameterMap;constructor(n,e){this.path=n,this.parameters=e}get parameterMap(){return this._parameterMap??=yo(this.parameters),this._parameterMap}toString(){return Fx(this)}};function hN(t,n){return xi(t,n)&&t.every((e,r)=>jn(e.parameters,n[r].parameters))}function xi(t,n){return t.length!==n.length?!1:t.every((e,r)=>e.path===n[r].path)}function gN(t,n){let e=[];return Object.entries(t.children).forEach(([r,i])=>{r===ee&&(e=e.concat(n(i,r)))}),Object.entries(t.children).forEach(([r,i])=>{r!==ee&&(e=e.concat(n(i,r)))}),e}var bd=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:()=>new wi})}return t})(),wi=class{parse(n){let e=new Hp(n);return new _n(e.parseRootSegment(),e.parseQueryParams(),e.parseFragment())}serialize(n){let e=`/${ls(n.root,!0)}`;bN(e)&&(e="/");let r=xN(n.queryParams),i=typeof n.fragment=="string"?`#${yN(n.fragment)}`:"";return`${e}${r}${i}`}},_x="http://fake";function bN(t){try{let n=new URL(t,_x);return n.origin!==_x||n.pathname.startsWith("//")}catch{return!0}}var vN=new wi;function id(t){return t.segments.map(n=>Fx(n)).join("/")}function ls(t,n){if(!t.hasChildren())return id(t);if(n){let e=t.children[ee]?ls(t.children[ee],!1):"",r=[];return Object.entries(t.children).forEach(([i,o])=>{i!==ee&&r.push(`${i}:${ls(o,!1)}`)}),r.length>0?`${e}(${r.join("//")})`:e}else{let e=gN(t,(r,i)=>i===ee?[ls(t.children[ee],!1)]:[`${i}:${ls(r,!1)}`]);return Object.keys(t.children).length===1&&t.children[ee]!=null?`${id(t)}/${e[0]}`:`${id(t)}/(${e.join("//")})`}}function Ox(t){return encodeURIComponent(t).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function td(t){return Ox(t).replace(/%3B/gi,";")}function yN(t){return encodeURI(t)}function zp(t){return Ox(t).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function od(t){return decodeURIComponent(t)}function xx(t){return od(t.replace(/\+/g,"%20"))}function Fx(t){return`${zp(t.path)}${_N(t.parameters)}`}function _N(t){return Object.entries(t).map(([n,e])=>`;${zp(n)}=${zp(e)}`).join("")}function xN(t){let n=Object.entries(t).map(([e,r])=>Array.isArray(r)?r.map(i=>`${td(e)}=${td(i)}`).join("&"):`${td(e)}=${td(r)}`).filter(e=>e);return n.length?`?${n.join("&")}`:""}var Fp=1073741824;function ld(t,n,e){Number(n)>=32&&!Object.hasOwn(t,Fp)&&(t[Fp]=e,delete t[Fp]),t[n]=e}var wN=/^[^\/()?;#]+/;function Pp(t){let n=t.match(wN);return n?n[0]:""}var CN=/^[^\/()?;=#]+/;function DN(t){let n=t.match(CN);return n?n[0]:""}var SN=/^[^=?&#]+/;function EN(t){let n=t.match(SN);return n?n[0]:""}var kN=/^[^&#]+/;function IN(t){let n=t.match(kN);return n?n[0]:""}var Hp=class{url;remaining;constructor(n){this.url=n,this.remaining=n}parseRootSegment(){for(;this.consumeOptional("/"););return this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new ye([],{}):new ye([],this.parseChildren())}parseQueryParams(){let n={};if(this.consumeOptional("?"))do this.parseQueryParam(n);while(this.consumeOptional("&"));return n}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(n=0){if(n>50)throw new D(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let e=[];for(this.peekStartsWith("(")||e.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),e.push(this.parseSegment());let r={};this.peekStartsWith("/(")&&(this.capture("/"),r=this.parseParens(!0,n));let i={};return this.peekStartsWith("(")&&(i=this.parseParens(!1,n)),(e.length>0||Object.keys(r).length>0)&&(i[ee]=new ye(e,r)),i}parseSegment(){let n=Pp(this.remaining);if(n===""&&this.peekStartsWith(";"))throw new D(4009,!1);return this.capture(n),new _i(od(n),this.parseMatrixParams())}parseMatrixParams(){let n={};for(;this.consumeOptional(";");)this.parseParam(n);return n}parseParam(n){let e=DN(this.remaining);if(!e)return;this.capture(e);let r="";if(this.consumeOptional("=")){let i=Pp(this.remaining);i&&(r=i,this.capture(r))}ld(n,od(e),od(r))}parseQueryParam(n){let e=EN(this.remaining);if(!e)return;this.capture(e);let r="";if(this.consumeOptional("=")){let a=IN(this.remaining);a&&(r=a,this.capture(r))}let i=xx(e),o=xx(r);if(Object.hasOwn(n,i)){let a=n[i];Array.isArray(a)||(a=[a],n[i]=a),a.push(o)}else n[i]=o}parseParens(n,e){let r=Object.create(null);for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let i=Pp(this.remaining),o=this.remaining[i.length];if(o!=="/"&&o!==")"&&o!==";")throw new D(4010,!1);let a;i.indexOf(":")>-1?(a=i.slice(0,i.indexOf(":")),this.capture(a),this.capture(":")):n&&(a=ee);let s=this.parseChildren(e+1),l=Object.keys(s).length===1&&s[ee]?s[ee]:new ye([],s);ld(r,a??ee,l),this.consumeOptional("//")}return r}peekStartsWith(n){return this.remaining.startsWith(n)}consumeOptional(n){return this.peekStartsWith(n)?(this.remaining=this.remaining.substring(n.length),!0):!1}capture(n){if(!this.consumeOptional(n))throw new D(4011,!1)}};function Px(t){return t.segments.length>0?new ye([],{[ee]:t}):t}function Lx(t){let n=Object.create(null);for(let[r,i]of Object.entries(t.children)){let o=Lx(i);if(r===ee&&o.segments.length===0&&o.hasChildren())for(let[a,s]of Object.entries(o.children))ld(n,a,s);else(o.segments.length>0||o.hasChildren())&&ld(n,r,o)}let e=new ye(t.segments,n);return MN(e)}function MN(t){if(t.numberOfChildren===1&&t.children[ee]){let n=t.children[ee];return new ye(t.segments.concat(n.segments),n.children)}return t}function _o(t){return t instanceof _n}function NN(t,n,e=null,r=null,i=new wi){let o=Vx(t);return jx(o,n,e,r,i)}function Vx(t){let n;function e(o){let a={};for(let l of o.children){let c=e(l);a[l.outlet]=c}let s=new ye(o.url,a);return o===t&&(n=s),s}let r=e(t.root),i=Px(r);return n??i}function jx(t,n,e,r,i){let o=t;for(;o.parent;)o=o.parent;if(n.length===0)return Lp(o,o,o,e,r,i);let a=TN(n);if(a.toRoot())return Lp(o,o,new ye([],{}),e,r,i);let s=AN(a,o,t),l=s.processChildren?ds(s.segmentGroup,s.index,a.commands):zx(s.segmentGroup,s.index,a.commands);return Lp(o,s.segmentGroup,l,e,r,i)}function cd(t){return typeof t=="object"&&t!=null&&!t.outlets&&!t.segmentPath}function ps(t){return typeof t=="object"&&t!=null&&t.outlets}function wx(t,n,e){t||="\u0275";let r=new _n;return r.queryParams={[t]:n},e.parse(e.serialize(r)).queryParams[t]}function Lp(t,n,e,r,i,o){let a={};for(let[c,d]of Object.entries(r??{}))a[c]=Array.isArray(d)?d.map(m=>wx(c,m,o)):wx(c,d,o);let s;t===n?s=e:s=Bx(t,n,e);let l=Px(Lx(s));return new _n(l,a,i)}function Bx(t,n,e){let r=Object.create(null);return Object.entries(t.children).forEach(([i,o])=>{o===n?r[i]=e:r[i]=Bx(o,n,e)}),new ye(t.segments,r)}var dd=class{isAbsolute;numberOfDoubleDots;commands;constructor(n,e,r){if(this.isAbsolute=n,this.numberOfDoubleDots=e,this.commands=r,n&&r.length>0&&cd(r[0]))throw new D(4003,!1);let i=r.find(ps);if(i&&i!==uN(r))throw new D(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function TN(t){if(typeof t[0]=="string"&&t.length===1&&t[0]==="/")return new dd(!0,0,t);let n=0,e=!1,r=t.reduce((i,o,a)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let s={};return Object.entries(o.outlets).forEach(([l,c])=>{s[l]=typeof c=="string"?c.split("/"):c}),[...i,{outlets:s}]}if(o.segmentPath)return[...i,o.segmentPath]}return typeof o!="string"?[...i,o]:a===0?(o.split("/").forEach((s,l)=>{l==0&&s==="."||(l==0&&s===""?e=!0:s===".."?n++:s!=""&&i.push(s))}),i):[...i,o]},[]);return new dd(e,n,r)}var bo=class{segmentGroup;processChildren;index;constructor(n,e,r){this.segmentGroup=n,this.processChildren=e,this.index=r}};function AN(t,n,e){if(t.isAbsolute)return new bo(n,!0,0);if(!e)return new bo(n,!1,NaN);if(e.parent===null)return new bo(e,!0,0);let r=cd(t.commands[0])?0:1,i=e.segments.length-1+r;return RN(e,i,t.numberOfDoubleDots)}function RN(t,n,e){let r=t,i=n,o=e;for(;o>i;){if(o-=i,r=r.parent,!r)throw new D(4005,!1);i=r.segments.length}return new bo(r,!1,i-o)}function ON(t){return ps(t[0])?t[0].outlets:{[ee]:t}}function zx(t,n,e){if(t??=new ye([],{}),t.segments.length===0&&t.hasChildren())return ds(t,n,e);let r=FN(t,n,e),i=e.slice(r.commandIndex);if(r.match&&r.pathIndex<t.segments.length){let o=new ye(t.segments.slice(0,r.pathIndex),{});return o.children[ee]=new ye(t.segments.slice(r.pathIndex),t.children),ds(o,0,i)}else return r.match&&i.length===0?new ye(t.segments,{}):r.match&&!t.hasChildren()?Up(t,n,e):r.match?ds(t,0,i):Up(t,n,e)}function ds(t,n,e){if(e.length===0)return new ye(t.segments,{});{let r=ON(e),i=Object.create(null);if(Object.keys(r).some(o=>o!==ee)&&t.children[ee]&&t.numberOfChildren===1&&t.children[ee].segments.length===0){let o=ds(t.children[ee],n,e);return new ye(t.segments,o.children)}return Object.entries(r).forEach(([o,a])=>{typeof a=="string"&&(a=[a]),a!==null&&(i[o]=zx(t.children[o],n,a))}),Object.entries(t.children).forEach(([o,a])=>{r[o]===void 0&&(i[o]=a)}),new ye(t.segments,i)}}function FN(t,n,e){let r=0,i=n,o={match:!1,pathIndex:0,commandIndex:0};for(;i<t.segments.length;){if(r>=e.length)return o;let a=t.segments[i],s=e[r];if(ps(s))break;let l=`${s}`,c=r<e.length-1?e[r+1]:null;if(i>0&&l===void 0)break;if(l&&c&&typeof c=="object"&&c.outlets===void 0){if(!Dx(l,c,a))return o;r+=2}else{if(!Dx(l,{},a))return o;r++}i++}return{match:!0,pathIndex:i,commandIndex:r}}function Up(t,n,e){let r=t.segments.slice(0,n),i=0;for(;i<e.length;){let o=e[i];if(ps(o)){let l=PN(o.outlets);return new ye(r,l)}if(i===0&&cd(e[0])){let l=t.segments[n];r.push(new _i(l.path,Cx(e[0]))),i++;continue}let a=ps(o)?o.outlets[ee]:`${o}`,s=i<e.length-1?e[i+1]:null;a&&s&&cd(s)?(r.push(new _i(a,Cx(s))),i+=2):(r.push(new _i(a,{})),i++)}return new ye(r,{})}function PN(t){let n={};return Object.entries(t).forEach(([e,r])=>{typeof r=="string"&&(r=[r]),r!==null&&(n[e]=Up(new ye([],{}),0,r))}),n}function Cx(t){let n={};return Object.entries(t).forEach(([e,r])=>n[e]=`${r}`),n}function Dx(t,n,e){return t==e.path&&jn(n,e.parameters)}var $p=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(n){this.rootInjector=n,this.children=new xo(this.rootInjector)}resetChildren(){this.children=new xo(this.rootInjector)}},xo=(()=>{class t{rootInjector;contexts=new Map;constructor(e){this.rootInjector=e}onChildOutletCreated(e,r){let i=this.getOrCreateContext(e);i.outlet=r,this.contexts.set(e,i)}onChildOutletDestroyed(e){let r=this.getContext(e);r&&(r.outlet=null,r.attachRef=null)}onOutletDeactivated(){let e=this.contexts;return this.contexts=new Map,e}onOutletReAttached(e){this.contexts=e}getOrCreateContext(e){let r=this.getContext(e);return r||(r=new $p(this.rootInjector),this.contexts.set(e,r)),r}getContext(e){return this.contexts.get(e)||null}static \u0275fac=function(r){return new(r||t)(X(Le))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),ud=class{_root;constructor(n){this._root=n}get root(){return this._root.value}parent(n){let e=this.pathFromRoot(n);return e.length>1?e[e.length-2]:null}children(n){let e=Gp(n,this._root);return e?e.children.map(r=>r.value):[]}firstChild(n){let e=Gp(n,this._root);return e&&e.children.length>0?e.children[0].value:null}siblings(n){let e=Wp(n,this._root);return e.length<2?[]:e[e.length-2].children.map(i=>i.value).filter(i=>i!==n)}pathFromRoot(n){return Wp(n,this._root).map(e=>e.value)}};function Gp(t,n){if(t===n.value)return n;for(let e of n.children){let r=Gp(t,e);if(r)return r}return null}function Wp(t,n){if(t===n.value)return[n];for(let e of n.children){let r=Wp(t,e);if(r.length)return r.unshift(n),r}return[]}var Et=class{value;children;constructor(n,e){this.value=n,this.children=e}toString(){return`TreeNode(${this.value})`}};function go(t){let n={};return t&&t.children.forEach(e=>n[e.value.outlet]=e),n}var md=class extends ud{snapshot;constructor(n,e){super(n),this.snapshot=e,fh(this,n)}toString(){return this.snapshot.toString()}};function Hx(t,n){let e=LN(t,n),r=new tt([new _i("",{})]),i=new tt({}),o=new tt({}),a=new tt({}),s=new tt(""),l=new Ci(r,i,a,s,o,ee,t,e.root);return l.snapshot=e.root,new md(new Et(l,[]),e)}function LN(t,n){let e={},r={},i={},a=new hs([],e,i,"",r,ee,t,null,{},n);return new fd("",new Et(a,[]))}var Ci=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;resources;_localInjector;pending;paramsSignal;queryParamsSignal;paramMapSignal;queryParamMapSignal;fragmentSignal;dataSignal;constructor(n,e,r,i,o,a,s,l){this.urlSubject=n,this.paramsSubject=e,this.queryParamsSubject=r,this.fragmentSubject=i,this.dataSubject=o,this.outlet=a,this.component=s,this._futureSnapshot=l,this.title=this.dataSubject?.pipe(_e(c=>c[ys]))??Y(void 0),this.url=n,this.params=e,this.queryParams=r,this.fragment=i,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(_e(n=>yo(n))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(_e(n=>yo(n))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}_setPending(n){this._futureSnapshot=n,this.pending?.set(!0)}},VN="always";function mh(t,n,e){let r,{routeConfig:i}=t;return n!==null&&(e==="always"||i?.path===""||!n.component&&!n.routeConfig?.loadComponent)?r={params:Object.keys(t.params).length===0?n.params:Object.freeze(g(g({},n.params),t.params)),data:Object.freeze(g(g({},n.data),t.data)),resolve:g(g(g(g({},t.data),n.data),i?.data),t._resolvedData)}:r={params:Object.freeze(g({},t.params)),data:Object.freeze(g({},t.data)),resolve:g(g({},t.data),t._resolvedData??{})},i&&$x(i)&&(r.resolve[ys]=i.title),r}var hs=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;resources;get title(){return this.data?.[ys]}constructor(n,e,r,i,o,a,s,l,c,d){this.url=n,this.params=e,this.queryParams=r,this.fragment=i,this.data=o,this.outlet=a,this.component=s,this.routeConfig=l,this._resolve=c,this._environmentInjector=d}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=yo(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=yo(this.queryParams),this._queryParamMap}toString(){let n=this.url.map(r=>r.toString()).join("/"),e=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${n}', path:'${e}')`}},fd=class extends ud{url;constructor(n,e){super(e),this.url=n,fh(this,e)}toString(){return Ux(this._root)}};function fh(t,n){n.value._routerState=t,n.children.forEach(e=>fh(t,e))}function Ux(t){let n=t.children.length>0?` { ${t.children.map(Ux).join(", ")} } `:"";return`${t.value}${n}`}function Vp(t){if(t.snapshot){let n=t.snapshot,e=t._futureSnapshot;t.snapshot=e,jn(n.queryParams,e.queryParams)||t.queryParamsSubject.next(e.queryParams),n.fragment!==e.fragment&&t.fragmentSubject.next(e.fragment),jn(n.params,e.params)||t.paramsSubject.next(e.params),dN(n.url,e.url)||t.urlSubject.next(e.url),jn(n.data,e.data)||t.dataSubject.next(e.data)}else t.snapshot=t._futureSnapshot,t.dataSubject.next(t._futureSnapshot.data)}function qp(t,n){let e=jn(t.params,n.params)&&hN(t.url,n.url),r=!t.parent!=!n.parent;return e&&!r&&(!t.parent||qp(t.parent,n.parent))}function $x(t){return typeof t.title=="string"||t.title===null}var jN=new b(""),Gx=(()=>{class t{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=ee;activateEvents=new C;deactivateEvents=new C;attachEvents=new C;detachEvents=new C;routerOutletData=po();parentContexts=u(xo);location=u(bt);changeDetector=u(ve);inputBinder=u(vd,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(e){if(e.name){let{firstChange:r,previousValue:i}=e.name;if(r)return;this.isTrackedInParentContexts(i)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(i)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(e){return this.parentContexts.getContext(e)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let e=this.parentContexts.getContext(this.name);e?.route&&(e.attachRef?this.attach(e.attachRef,e.route):this.activateWith(e.route,e.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new D(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new D(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new D(4012,!1);this.location.detach();let e=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(e.instance),e}attach(e,r){this.activated=e,this._activatedRoute=r,this.location.insert(e.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.attachEvents.emit(e.instance)}deactivate(){if(this.activated){let e=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(e)}}activateWith(e,r){if(this.isActivated)throw new D(4013,!1);this._activatedRoute=e;let i=this.location,a=e.snapshot.component,s=this.parentContexts.getOrCreateContext(this.name).children,l=new Yp(e,s,i.injector,this.routerOutletData);this.activated=i.createComponent(a,{index:i.length,injector:l,environmentInjector:r}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this,this.location.injector),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Ae]})}return t})(),Yp=class{route;childContexts;parent;outletData;constructor(n,e,r,i){this.route=n,this.childContexts=e,this.parent=r,this.outletData=i}get(n,e){return n===Ci?this.route:n===xo?this.childContexts:n===jN?this.outletData:this.parent.get(n,e)}},vd=new b("");var Wx=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(r,i){r&1&&Z(0,"router-outlet")},dependencies:[Gx],encapsulation:2,changeDetection:1})}return t})();function ph(t){let n=t.children&&t.children.map(ph),e=n?R(g({},t),{children:n}):g({},t);return!e.component&&!e.loadComponent&&(n||e.loadChildren)&&e.outlet&&e.outlet!==ee&&(e.component=Wx),e}var us="imperative",pt=(function(t){return t[t.NavigationStart=0]="NavigationStart",t[t.NavigationEnd=1]="NavigationEnd",t[t.NavigationCancel=2]="NavigationCancel",t[t.NavigationError=3]="NavigationError",t[t.RoutesRecognized=4]="RoutesRecognized",t[t.ResolveStart=5]="ResolveStart",t[t.ResolveEnd=6]="ResolveEnd",t[t.GuardsCheckStart=7]="GuardsCheckStart",t[t.GuardsCheckEnd=8]="GuardsCheckEnd",t[t.RouteConfigLoadStart=9]="RouteConfigLoadStart",t[t.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",t[t.ChildActivationStart=11]="ChildActivationStart",t[t.ChildActivationEnd=12]="ChildActivationEnd",t[t.ActivationStart=13]="ActivationStart",t[t.ActivationEnd=14]="ActivationEnd",t[t.Scroll=15]="Scroll",t[t.NavigationSkipped=16]="NavigationSkipped",t})(pt||{}),Jt=class{id;url;constructor(n,e){this.id=n,this.url=e}},wo=class extends Jt{type=pt.NavigationStart;navigationTrigger;restoredState;constructor(n,e,r="imperative",i=null){super(n,e),this.navigationTrigger=r,this.restoredState=i}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},Sr=class extends Jt{urlAfterRedirects;type=pt.NavigationEnd;constructor(n,e,r){super(n,e),this.urlAfterRedirects=r}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},vt=(function(t){return t[t.Redirect=0]="Redirect",t[t.SupersededByNewNavigation=1]="SupersededByNewNavigation",t[t.NoDataFromResolver=2]="NoDataFromResolver",t[t.GuardRejected=3]="GuardRejected",t[t.Aborted=4]="Aborted",t})(vt||{}),pd=(function(t){return t[t.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",t[t.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",t})(pd||{}),vn=class extends Jt{reason;code;type=pt.NavigationCancel;constructor(n,e,r,i){super(n,e),this.reason=r,this.code=i}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function qx(t){return t instanceof vn&&(t.code===vt.Redirect||t.code===vt.SupersededByNewNavigation)}var Er=class extends Jt{reason;code;type=pt.NavigationSkipped;constructor(n,e,r,i){super(n,e),this.reason=r,this.code=i}},Co=class extends Jt{error;target;type=pt.NavigationError;constructor(n,e,r,i){super(n,e),this.error=r,this.target=i}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},hd=class extends Jt{urlAfterRedirects;state;type=pt.RoutesRecognized;constructor(n,e,r,i){super(n,e),this.urlAfterRedirects=r,this.state=i}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Kp=class extends Jt{urlAfterRedirects;state;type=pt.GuardsCheckStart;constructor(n,e,r,i){super(n,e),this.urlAfterRedirects=r,this.state=i}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Zp=class extends Jt{urlAfterRedirects;state;shouldActivate;type=pt.GuardsCheckEnd;constructor(n,e,r,i,o){super(n,e),this.urlAfterRedirects=r,this.state=i,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Qp=class extends Jt{urlAfterRedirects;state;type=pt.ResolveStart;constructor(n,e,r,i){super(n,e),this.urlAfterRedirects=r,this.state=i}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Xp=class extends Jt{urlAfterRedirects;state;type=pt.ResolveEnd;constructor(n,e,r,i){super(n,e),this.urlAfterRedirects=r,this.state=i}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Jp=class{route;type=pt.RouteConfigLoadStart;constructor(n){this.route=n}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},eh=class{route;type=pt.RouteConfigLoadEnd;constructor(n){this.route=n}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},th=class{snapshot;type=pt.ChildActivationStart;constructor(n){this.snapshot=n}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},nh=class{snapshot;type=pt.ChildActivationEnd;constructor(n){this.snapshot=n}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},rh=class{snapshot;type=pt.ActivationStart;constructor(n){this.snapshot=n}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},ih=class{snapshot;type=pt.ActivationEnd;constructor(n){this.snapshot=n}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var Do=class{},gs=class{},So=class{url;navigationBehaviorOptions;constructor(n,e){this.url=n,this.navigationBehaviorOptions=e}};function BN(t){return!(t instanceof Do)&&!(t instanceof So)&&!(t instanceof gs)}function zN(t,n,e){let r=new Set,i=bs(t,n._root,e?e._root:void 0,r);return{newlyCreatedRoutes:r,state:new md(i,n)}}function bs(t,n,e,r){if(e&&t.shouldReuseRoute(n.value,e.value.snapshot)){let i=e.value;i._setPending(n.value);let o=HN(t,n,e,r);return new Et(i,o)}else{if(t.shouldAttach(n.value)){let a=t.retrieve(n.value);if(a!==null){let s=a.route;return s.value._setPending(n.value),s.children=n.children.map(l=>bs(t,l,void 0,r)),s}}let i=UN(n.value);i._setPending(n.value),r.add(i);let o=n.children.map(a=>bs(t,a,void 0,r));return new Et(i,o)}}function HN(t,n,e,r){return n.children.map(i=>{for(let o of e.children)if(t.shouldReuseRoute(i.value,o.value.snapshot))return bs(t,i,o,r);return bs(t,i,void 0,r)})}function UN(t){return new Ci(new tt(t.url),new tt(t.params),new tt(t.queryParams),new tt(t.fragment),new tt(t.data),t.outlet,t.component,t)}var Eo=class t extends Error{redirectTo;navigationBehaviorOptions;constructor(n,e){super(),this.redirectTo=n,this.navigationBehaviorOptions=e,Object.setPrototypeOf(this,t.prototype)}},Yx="ngNavigationCancelingError";function ms(t,n){let{redirectTo:e,navigationBehaviorOptions:r}=_o(n)?{redirectTo:n,navigationBehaviorOptions:void 0}:n,i=Kx(!1,vt.Redirect);return i.url=e,i.navigationBehaviorOptions=r,i}function Kx(t,n){let e=new Error(`NavigationCancelingError: ${t||""}`);return e[Yx]=!0,e.cancellationCode=n,e}function $N(t){return Zx(t)&&_o(t.url)}function Zx(t){return!!t&&t[Yx]}var oh=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(n,e,r,i,o){this.routeReuseStrategy=n,this.futureState=e,this.currState=r,this.forwardEvent=i,this.inputBindingEnabled=o}activate(n){let e=this.futureState._root,r=this.currState?this.currState._root:null;this.deactivateChildRoutes(e,r,n),Vp(this.futureState.root),this.activateChildRoutes(e,r,n)}deactivateChildRoutes(n,e,r){let i=go(e);n.children.forEach(o=>{let a=o.value.outlet;this.deactivateRoutes(o,i[a],r),delete i[a]}),Object.values(i).forEach(o=>{this.deactivateRouteAndItsChildren(o,r)})}deactivateRoutes(n,e,r){let i=n.value,o=e?e.value:null;if(i===o)if(i.component){let a=r.getContext(i.outlet);a&&this.deactivateChildRoutes(n,e,a.children)}else this.deactivateChildRoutes(n,e,r);else o&&this.deactivateRouteAndItsChildren(e,r)}deactivateRouteAndItsChildren(n,e){n.value.component&&this.routeReuseStrategy.shouldDetach(n.value.snapshot)?this.detachAndStoreRouteSubtree(n,e):this.deactivateRouteAndOutlet(n,e)}detachAndStoreRouteSubtree(n,e){let r=e.getContext(n.value.outlet),i=r&&n.value.component?r.children:e,o=go(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,i);if(r&&r.outlet){let a=r.outlet.detach(),s=r.children.contexts;r.resetChildren(),this.routeReuseStrategy.store(n.value.snapshot,{componentRef:a,route:n,contexts:s})}}deactivateRouteAndOutlet(n,e){let r=e.getContext(n.value.outlet),i=r&&n.value.component?r.children:e,o=go(n);for(let a of Object.values(o))this.deactivateRouteAndItsChildren(a,i);r&&(r.outlet&&(r.outlet.deactivate(),r.children.onOutletDeactivated()),r.attachRef=null,r.route=null),n.value._localInjector?.destroy()}activateChildRoutes(n,e,r){let i=go(e);n.children.forEach(o=>{this.activateRoutes(o,i[o.value.outlet],r),this.forwardEvent(new ih(o.value.snapshot))}),n.children.length&&this.forwardEvent(new nh(n.value.snapshot))}activateRoutes(n,e,r){let i=n.value,o=e?e.value:null;if(Vp(i),i===o)if(i.component){let a=r.getOrCreateContext(i.outlet);this.activateChildRoutes(n,e,a.children)}else this.activateChildRoutes(n,e,r);else if(i.component){let a=r.getOrCreateContext(i.outlet);if(this.routeReuseStrategy.shouldAttach(i.snapshot)){let s=this.routeReuseStrategy.retrieve(i.snapshot);this.routeReuseStrategy.store(i.snapshot,null),a.children.onOutletReAttached(s.contexts),a.attachRef=s.componentRef,a.route=s.route.value,a.outlet&&a.outlet.attach(s.componentRef,s.route.value),Vp(s.route.value),this.activateChildRoutes(n,null,a.children)}else a.attachRef=null,a.route=i,a.outlet&&a.outlet.activateWith(i,a.injector),this.activateChildRoutes(n,null,a.children)}else this.activateChildRoutes(n,null,r)}},gd=class{path;route;constructor(n){this.path=n,this.route=this.path[this.path.length-1]}},vo=class{component;route;constructor(n,e){this.component=n,this.route=e}};function GN(t,n,e){let r=t._root,i=n?n._root:null;return cs(r,i,e,[r.value])}function WN(t){let n=t.routeConfig?t.routeConfig.canActivateChild:null;return!n||n.length===0?null:{node:t,guards:n}}function Io(t,n){let e=Symbol(),r=n.get(t,e);return r===e?typeof t=="function"&&!om(t)?t:n.get(t):r}function cs(t,n,e,r,i={canDeactivateChecks:[],canActivateChecks:[]}){let o=go(n);return t.children.forEach(a=>{qN(a,o[a.value.outlet],e,r.concat([a.value]),i),delete o[a.value.outlet]}),Object.entries(o).forEach(([a,s])=>fs(s,e.getContext(a),e,i)),i}function qN(t,n,e,r,i={canDeactivateChecks:[],canActivateChecks:[]}){let o=t.value,a=n?n.value:null,s=e?e.getContext(t.value.outlet):null;if(a&&o.routeConfig===a.routeConfig){let l=YN(a,o,o.routeConfig.runGuardsAndResolvers);l?i.canActivateChecks.push(new gd(r)):(o.data=a.data,o._resolvedData=a._resolvedData),o.component?cs(t,n,s?s.children:null,r,i):cs(t,n,e,r,i),l&&s&&s.outlet&&s.outlet.isActivated&&i.canDeactivateChecks.push(new vo(s.outlet.component,a))}else a&&fs(n,s,e,i),i.canActivateChecks.push(new gd(r)),o.component?cs(t,null,s?s.children:null,r,i):cs(t,null,e,r,i);return i}function YN(t,n,e){if(typeof e=="function")return st(n._environmentInjector,()=>e(t,n));switch(e){case"pathParamsChange":return!xi(t.url,n.url);case"pathParamsOrQueryParamsChange":return!xi(t.url,n.url)||!jn(t.queryParams,n.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!qp(t,n)||!jn(t.queryParams,n.queryParams);default:return!qp(t,n)}}function fs(t,n,e,r){let i=go(t),o=t.value;Object.entries(i).forEach(([a,s])=>{o.component?n?fs(s,n.children.getContext(a),n.children,r):fs(s,null,null,r):fs(s,e?e.getContext(a):null,e,r)}),o.component?n&&n.outlet&&n.outlet.isActivated?r.canDeactivateChecks.push(new vo(n.outlet.component,o)):r.canDeactivateChecks.push(new vo(null,o)):r.canDeactivateChecks.push(new vo(null,o))}function _s(t){return typeof t=="function"}function KN(t){return typeof t=="boolean"}function ZN(t){return t&&_s(t.canLoad)}function QN(t){return t&&_s(t.canActivate)}function XN(t){return t&&_s(t.canActivateChild)}function JN(t){return t&&_s(t.canDeactivate)}function eT(t){return t&&_s(t.canMatch)}function Qx(t){return t instanceof Gr||t?.name==="EmptyError"}var nd=Symbol("INITIAL_VALUE");function ko(){return Pt(t=>ua(t.map(n=>n.pipe(Ft(1),Ht(nd)))).pipe(_e(n=>{for(let e of n)if(e!==!0){if(e===nd)return nd;if(e===!1||tT(e))return e}return!0}),Ye(n=>n!==nd),Ft(1)))}function tT(t){return _o(t)||t instanceof Eo}function Xx(t){return t.aborted?Y(void 0).pipe(Ft(1)):new q(n=>{let e=()=>{n.next(),n.complete()};return t.addEventListener("abort",e),()=>t.removeEventListener("abort",e)})}function Jx(t){return Ue(Xx(t))}function nT(t){return Ct(n=>{let{targetSnapshot:e,currentSnapshot:r,guards:{canActivateChecks:i,canDeactivateChecks:o}}=n;return o.length===0&&i.length===0?Y(R(g({},n),{guardsResult:!0})):rT(o,e,r).pipe(Ct(a=>a&&KN(a)?iT(e,i,t):Y(a)),_e(a=>R(g({},n),{guardsResult:a})))})}function rT(t,n,e){return Pe(t).pipe(Ct(r=>cT(r.component,r.route,e,n)),Hn(r=>r!==!0,!0))}function iT(t,n,e){return Pe(n).pipe(Il(r=>cr(aT(r.route.parent,e),oT(r.route,e),lT(t,r.path),sT(t,r.route))),Hn(r=>r!==!0,!0))}function oT(t,n){return t!==null&&n&&n(new rh(t)),Y(!0)}function aT(t,n){return t!==null&&n&&n(new th(t)),Y(!0)}function sT(t,n){let e=n.routeConfig?n.routeConfig.canActivate:null;if(!e||e.length===0)return Y(!0);let r=e.map(i=>ma(()=>{let o=n._environmentInjector,a=Io(i,o),s=QN(a)?a.canActivate(n,t):st(o,()=>a(n,t));return Di(s).pipe(Hn())}));return Y(r).pipe(ko())}function lT(t,n){let e=n[n.length-1],i=n.slice(0,n.length-1).reverse().map(o=>WN(o)).filter(o=>o!==null).map(o=>ma(()=>{let a=o.guards.map(s=>{let l=o.node._environmentInjector,c=Io(s,l),d=XN(c)?c.canActivateChild(e,t):st(l,()=>c(e,t));return Di(d).pipe(Hn())});return Y(a).pipe(ko())}));return Y(i).pipe(ko())}function cT(t,n,e,r){let i=n&&n.routeConfig?n.routeConfig.canDeactivate:null;if(!i||i.length===0)return Y(!0);let o=i.map(a=>{let s=n._environmentInjector,l=Io(a,s),c=JN(l)?l.canDeactivate(t,n,e,r):st(s,()=>l(t,n,e,r));return Di(c).pipe(Hn())});return Y(o).pipe(ko())}function dT(t,n,e,r,i){let o=n.canLoad;if(o===void 0||o.length===0)return Y(!0);let a=o.map(s=>{let l=Io(s,t),c=ZN(l)?l.canLoad(n,e):st(t,()=>l(n,e)),d=Di(c);return i?d.pipe(Jx(i)):d});return Y(a).pipe(ko(),ew(r))}function ew(t){return rl(Ut(n=>{if(typeof n!="boolean")throw ms(t,n)}),_e(n=>n===!0))}function uT(t,n,e,r,i,o){let a=n.canMatch;if(!a||a.length===0)return Y(!0);let s=a.map(l=>{let c=Io(l,t),d=eT(c)?c.canMatch(n,e,i):st(t,()=>c(n,e,i));return Di(d).pipe(Jx(o))});return Y(s).pipe(ko(),ew(r))}var Vn=class t extends Error{name="NoMatch";segmentGroup;constructor(n){super(),this.segmentGroup=n||null,Object.setPrototypeOf(this,t.prototype)}},vs=class t extends Error{urlTree;name="AbsoluteRedirect";constructor(n){super(),this.urlTree=n,Object.setPrototypeOf(this,t.prototype)}};function mT(t){throw new D(4e3,!1)}function fT(t){throw Kx(!1,vt.GuardRejected)}var ah=class{urlSerializer;urlTree;constructor(n,e){this.urlSerializer=n,this.urlTree=e}async lineralizeSegments(n,e){let r=[],i=e.root;for(;;){if(r=r.concat(i.segments),i.numberOfChildren===0)return r;if(i.numberOfChildren>1||!i.children[ee])throw mT(`${n.redirectTo}`);i=i.children[ee]}}async applyRedirectCommands(n,e,r,i,o){let a=await pT(e,i,o);if(a instanceof _n)throw new vs(a);let s=this.applyRedirectCreateUrlTree(a,this.urlSerializer.parse(a),n,r);if(a[0]==="/")throw new vs(s);return s}applyRedirectCreateUrlTree(n,e,r,i){let o=this.createSegmentGroup(n,e.root,r,i);return new _n(o,this.createQueryParams(e.queryParams,this.urlTree.queryParams),e.fragment)}createQueryParams(n,e){let r={};return Object.entries(n).forEach(([i,o])=>{if(typeof o=="string"&&o[0]===":"){let s=o.substring(1);r[i]=e[s]}else r[i]=o}),r}createSegmentGroup(n,e,r,i){let o=this.createSegments(n,e.segments,r,i),a=Object.create(null);return Object.entries(e.children).forEach(([s,l])=>{a[s]=this.createSegmentGroup(n,l,r,i)}),new ye(o,a)}createSegments(n,e,r,i){return e.map(o=>o.path[0]===":"?this.findPosParam(n,o,i):this.findOrReturn(o,r))}findPosParam(n,e,r){let i=r[e.path.substring(1)];if(!i)throw new D(4001,!1);return i}findOrReturn(n,e){let r=0;for(let i of e){if(i.path===n.path)return e.splice(r),i;r++}return n}};function pT(t,n,e){if(typeof t=="string")return Promise.resolve(t);let r=t;return ad(Di(st(e,()=>r(n))))}function hT(t,n){return t.providers&&!t._injector&&(t._injector=Ka(t.providers,n,`Route: ${t.path}`)),t._injector??n}function yn(t){return t.outlet||ee}function gT(t,n){let e=t.filter(r=>yn(r)===n);return e.push(...t.filter(r=>yn(r)!==n)),e}var sh={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function tw(t){return{routeConfig:t.routeConfig,url:t.url,params:t.params,queryParams:t.queryParams,fragment:t.fragment,data:t.data,outlet:t.outlet,title:t.title,paramMap:t.paramMap,queryParamMap:t.queryParamMap}}function bT(t,n,e,r,i,o,a){let s=nw(t,n,e);if(!s.matched)return Y(s);let l=tw(o(s));return r=hT(n,r),uT(r,n,e,i,l,a).pipe(_e(c=>c===!0?s:g({},sh)))}function nw(t,n,e){if(n.path==="")return n.pathMatch==="full"&&(t.hasChildren()||e.length>0)?g({},sh):{matched:!0,consumedSegments:[],remainingSegments:e,parameters:{},positionalParamSegments:{}};let i=(n.matcher||cN)(e,t,n);if(!i)return g({},sh);let o={};Object.entries(i.posParams??{}).forEach(([s,l])=>{o[s]=l.path});let a=i.consumed.length>0?g(g({},o),i.consumed[i.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:i.consumed,remainingSegments:e.slice(i.consumed.length),parameters:a,positionalParamSegments:i.posParams??{}}}function Sx(t,n,e,r,i){return e.length>0&&_T(t,e,r,i)?{segmentGroup:new ye(n,yT(r,new ye(e,t.children))),slicedSegments:[]}:e.length===0&&xT(t,e,r)?{segmentGroup:new ye(t.segments,vT(t,e,r,t.children)),slicedSegments:e}:{segmentGroup:new ye(t.segments,t.children),slicedSegments:e}}function vT(t,n,e,r){let i={};for(let o of e)if(yd(t,n,o)&&!r[yn(o)]){let a=new ye([],{});i[yn(o)]=a}return g(g({},r),i)}function yT(t,n){let e={};e[ee]=n;for(let r of t)if(r.path===""&&yn(r)!==ee){let i=new ye([],{});e[yn(r)]=i}return e}function _T(t,n,e,r){return e.some(i=>!yd(t,n,i)||!(yn(i)!==ee)?!1:!(r!==void 0&&yn(i)===r))}function xT(t,n,e){return e.some(r=>yd(t,n,r))}function yd(t,n,e){return(t.hasChildren()||n.length>0)&&e.pathMatch==="full"?!1:e.path===""}function wT(t,n,e){return n.length===0&&!t.hasChildren()}var lh=class{};async function CT(t,n,e,r,i,o,a,s){return new ch(t,n,e,r,i,a,o,s).recognize()}var DT=31,ch=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;queryParams;constructor(n,e,r,i,o,a,s,l){this.injector=n,this.configLoader=e,this.rootComponentType=r,this.config=i,this.urlTree=o,this.paramsInheritanceStrategy=a,this.urlSerializer=s,this.abortSignal=l,this.applyRedirects=new ah(this.urlSerializer,this.urlTree),this.queryParams=Object.freeze(g({},this.urlTree.queryParams))}noMatchError(n){return new D(4002,`'${n.segmentGroup}'`)}async recognize(){let n=Sx(this.urlTree.root,[],[],this.config).segmentGroup,{children:e,rootSnapshot:r}=await this.match(n),i=new Et(r,e),o=new fd("",i),a=NN(r,[],this.urlTree.queryParams,this.urlTree.fragment);return a.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(a),{state:o,tree:a}}async match(n){let e=new hs([],Object.freeze({}),this.queryParams,this.urlTree.fragment,Object.freeze({}),ee,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,n,ee,e),rootSnapshot:e}}catch(r){if(r instanceof vs)return this.absoluteRedirectCount++,this.absoluteRedirectCount>DT&&(this.allowRedirects=!1),this.urlTree=r.urlTree,this.queryParams=Object.freeze(g({},this.urlTree.queryParams)),this.match(r.urlTree.root);throw r instanceof Vn?this.noMatchError(r):r}}async processSegmentGroup(n,e,r,i,o){if(r.segments.length===0&&r.hasChildren())return this.processChildren(n,e,r,o);let a=await this.processSegment(n,e,r,r.segments,i,!0,o);return a instanceof Et?[a]:[]}async processChildren(n,e,r,i){let o=[];for(let l of Object.keys(r.children))l==="primary"?o.unshift(l):o.push(l);let a=[];for(let l of o){let c=r.children[l],d=gT(e,l),m=await this.processSegment(n,d,c,c.segments,l,!0,i);m instanceof Et&&a.push(m)}let s=rw(a);return ST(s),s}async processSegment(n,e,r,i,o,a,s){for(let l of e)try{return await this.processSegmentAgainstRoute(l._injector??n,e,l,r,i,o,a,s)}catch(c){if(c instanceof Vn||Qx(c))continue;throw c}if(wT(r,i))return new lh;throw new Vn(r)}async processSegmentAgainstRoute(n,e,r,i,o,a,s,l){if(yn(r)!==a&&(a===ee||!yd(i,o,r)||!r.children?.length&&!r.loadChildren||o.length===0&&!i.hasChildren()))throw new Vn(i);if(r.redirectTo===void 0)return this.matchSegmentAgainstRoute(n,i,r,o,a,l);if(this.allowRedirects&&s)return this.expandSegmentAgainstRouteUsingRedirect(n,i,e,r,o,a,l);throw new Vn(i)}async expandSegmentAgainstRouteUsingRedirect(n,e,r,i,o,a,s){let{matched:l,parameters:c,consumedSegments:d,positionalParamSegments:m,remainingSegments:p}=nw(e,i,o);if(!l)throw new Vn(e);let f=this.createSnapshot(n,i,o,c,s);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let h=await this.applyRedirects.applyRedirectCommands(d,i.redirectTo,m,tw(f),n),w=await this.applyRedirects.lineralizeSegments(i,h);return this.processSegment(n,r,e,w.concat(p),a,!1,s)}createSnapshot(n,e,r,i,o){let a=new hs(r,i,this.queryParams,this.urlTree.fragment,IT(e),yn(e),e.component??e._loadedComponent??null,e,MT(e),n),s=mh(a,o,this.paramsInheritanceStrategy);return a.params=s.params,a.data=s.data,a}async matchSegmentAgainstRoute(n,e,r,i,o,a){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let s=Je=>this.createSnapshot(n,r,Je.consumedSegments,Je.parameters,a),l=await ad(bT(e,r,i,n,this.urlSerializer,s,this.abortSignal));if(r.path==="**"&&(e.children={}),!l?.matched)throw new Vn(e);n=r._injector??n;let{routes:c}=await this.getChildConfig(n,r,i),d=r._loadedInjector??n,{parameters:m,consumedSegments:p,remainingSegments:f}=l,h=this.createSnapshot(n,r,p,m,a),{segmentGroup:w,slicedSegments:I}=Sx(e,p,f,c,o),P=yn(r)===o;if(P&&I.length===0&&w.hasChildren()){let Je=await this.processChildren(d,c,w,h);return new Et(h,Je)}if(P&&c.length===0&&I.length===0)return new Et(h,[]);let $=await this.processSegment(d,c,w,I,P?ee:o,!0,h);if(!P&&!($ instanceof Et))throw new Vn(e);return new Et(h,$ instanceof Et?[$]:[])}async getChildConfig(n,e,r){if(e.children)return{routes:e.children,injector:n};if(e.loadChildren){if(e._loadedRoutes!==void 0){let o=e._loadedNgModuleFactory;return o&&!e._loadedInjector&&(e._loadedInjector=o.create(n).injector),{routes:e._loadedRoutes,injector:e._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await ad(dT(n,e,r,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(n,e);return e._loadedRoutes=o.routes,e._loadedInjector=o.injector,e._loadedNgModuleFactory=o.factory,o}throw fT(e)}return{routes:[],injector:n}}};function ST(t){t.sort((n,e)=>n.value.outlet===ee?-1:e.value.outlet===ee?1:n.value.outlet.localeCompare(e.value.outlet))}function ET(t){let n=t.value.routeConfig;return n&&n.path===""}function rw(t){let n=[],e=new Set;for(let i of t){if(!ET(i)){n.push(i);continue}let o=n.find(a=>i.value.routeConfig===a.value.routeConfig);o!==void 0?(o.children.push(...i.children),e.add(o)):n.push(i)}for(let i of e){let o=rw(i.children);n.push(new Et(i.value,o))}let r=n.filter(i=>!e.has(i));return kT(r),r}function kT(t){let n=Object.create(null);t.forEach(e=>{let r=n[e.value.outlet];if(r){let i=r.url.map(a=>a.toString()).join("/"),o=e.value.url.map(a=>a.toString()).join("/");throw new D(4006,!1)}n[e.value.outlet]=e.value})}function IT(t){return t.data||{}}function MT(t){return t.resolve||{}}function NT(t,n,e,r,i,o,a){return Ct(async s=>{let{state:l,tree:c}=await CT(t,n,e,r,s.extractedUrl,i,o,a);return R(g({},s),{targetSnapshot:l,urlAfterRedirects:c})})}function TT(t){return Ct(n=>{let{targetSnapshot:e,guards:{canActivateChecks:r}}=n;if(!r.length)return Y(n);let i=new Set(r.map(s=>s.route)),o=new Set;for(let s of i)if(!o.has(s))for(let l of iw(s))o.add(l);let a=0;return Pe(o).pipe(Il(s=>i.has(s)?AT(s,e,t):(s.data=mh(s,s.parent,t).resolve,Y(void 0))),Ut(()=>a++),Nl(1),Ct(s=>a===o.size?Y(n):qe))})}function iw(t){let n=t.children.map(e=>iw(e)).flat();return[t,...n]}function AT(t,n,e){let r=t.routeConfig,i=t._resolve;return r?.title!==void 0&&!$x(r)&&(i[ys]=r.title),ma(()=>(t.data=mh(t,t.parent,e).resolve,RT(i,t,n).pipe(_e(o=>(t._resolvedData=o,t.data=g(g({},t.data),o),null)))))}function RT(t,n,e){let r=Bp(t);if(r.length===0)return Y({});let i={};return Pe(r).pipe(Ct(o=>OT(t[o],n,e).pipe(Hn(),Ut(a=>{if(a instanceof Eo)throw ms(new wi,a);i[o]=a}))),Nl(1),_e(()=>i),fa(o=>Qx(o)?qe:Bu(o)))}function OT(t,n,e){let r=n._environmentInjector,i=Io(t,r),o=i.resolve?i.resolve(n,e):st(r,()=>i(n,e));return Di(o)}var ow=new b("");function dh(t){return Pt(n=>{let e=t(n);return e?Pe(e).pipe(_e(()=>n)):Y(n)})}var aw=(()=>{class t{buildTitle(e){let r,i=e.root;for(;i!==void 0;)r=this.getResolvedTitleForRoute(i)??r,i=i.children.find(o=>o.outlet===ee);return r}getResolvedTitleForRoute(e){return e.data[ys]}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:()=>u(FT)})}return t})(),FT=(()=>{class t extends aw{title;constructor(e){super(),this.title=e}updateTitle(e){let r=this.buildTitle(e);r!==void 0&&this.title.setTitle(r)}static \u0275fac=function(r){return new(r||t)(X(vx))};static \u0275prov=me({token:t,factory:t.\u0275fac,providedIn:"root"})}return t})(),_d=new b("",{factory:()=>({})}),xd=new b(""),sw=(()=>{class t{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=u(mp);async loadComponent(e,r){if(this.componentLoaders.get(r))return this.componentLoaders.get(r);if(r._loadedComponent)return Promise.resolve(r._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(r);let i=(async()=>{try{let o=await Ix(st(e,()=>r.loadComponent())),a=await lw(bp(o));return this.onLoadEndListener&&this.onLoadEndListener(r),r._loadedComponent=a,a}finally{this.componentLoaders.delete(r)}})();return this.componentLoaders.set(r,i),i}loadChildren(e,r){if(this.childrenLoaders.get(r))return this.childrenLoaders.get(r);if(r._loadedRoutes)return Promise.resolve({routes:r._loadedRoutes,injector:r._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(r);let i=(async()=>{try{let o=await PT(r,this.compiler,e,this.onLoadEndListener);return r._loadedRoutes=o.routes,r._loadedInjector=o.injector,r._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(r)}})();return this.childrenLoaders.set(r,i),i}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();async function PT(t,n,e,r){let i=await Ix(st(e,()=>t.loadChildren())),o=await lw(bp(i)),a;o instanceof Bc||Array.isArray(o)?a=o:a=await n.compileModuleAsync(o),r&&r(t);let s,l,c=!1,d;return Array.isArray(a)?(l=a,c=!0):(s=a.create(e).injector,d=a,l=s.get(xd,[],{optional:!0,self:!0}).flat()),{routes:l.map(ph),injector:s,factory:d}}async function lw(t){return t}var hh=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:()=>u(LT)})}return t})(),LT=(()=>{class t{shouldProcessUrl(e){return!0}extract(e){return e}merge(e,r){return e}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),cw=new b("");var VT=()=>{},dw=new b(""),uw=(()=>{class t{currentNavigation=A(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=A(null);events=new E;transitionAbortWithErrorSubject=new E;configLoader=u(sw);environmentInjector=u(Le);destroyRef=u(at);urlSerializer=u(bd);rootContexts=u(xo);location=u(Dr);inputBindingEnabled=u(vd,{optional:!0})!==null;titleStrategy=u(aw);options=u(_d,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||VN;urlHandlingStrategy=u(hh);createViewTransition=u(cw,{optional:!0});navigationErrorHandler=u(dw,{optional:!0});routerResourcesFeature=u(ow,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>Y(void 0);rootComponentType=null;destroyed=!1;constructor(){let e=i=>this.events.next(new Jp(i)),r=i=>this.events.next(new eh(i));this.configLoader.onLoadEndListener=r,this.configLoader.onLoadStartListener=e,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(e){let r=++this.navigationId;Ie(()=>{this.transitions?.next(R(g({},e),{extractedUrl:this.urlHandlingStrategy.extract(e.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:r,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(e){return this.transitions=new tt(null),this.transitions.pipe(Ye(r=>r!==null),Pt(r=>{let i=!0,o=!1,a=new AbortController,s=()=>!o&&this.currentTransition?.id===r.id;return Y(r).pipe(Pt(l=>{if(this.navigationId>r.id)return this.cancelNavigationTransition(r,"",vt.SupersededByNewNavigation),qe;this.currentTransition=r;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:l.id,initialUrl:l.rawUrl,extractedUrl:l.extractedUrl,targetBrowserUrl:typeof l.extras.browserUrl=="string"?this.urlSerializer.parse(l.extras.browserUrl):l.extras.browserUrl,trigger:l.source,extras:l.extras,previousNavigation:c?R(g({},c),{previousNavigation:null}):null,abort:()=>a.abort(),routesRecognizeHandler:l.routesRecognizeHandler,beforeActivateHandler:l.beforeActivateHandler});let d=!e.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),m=l.extras.onSameUrlNavigation??e.onSameUrlNavigation;if(!d&&m!=="reload")return this.events.next(new Er(l.id,this.urlSerializer.serialize(l.rawUrl),"",pd.IgnoredSameUrlNavigation)),l.resolve(!1),qe;if(this.urlHandlingStrategy.shouldProcessUrl(l.rawUrl))return Y(l).pipe(Pt(p=>(this.events.next(new wo(p.id,this.urlSerializer.serialize(p.extractedUrl),p.source,p.restoredState)),p.id!==this.navigationId?qe:Promise.resolve(p))),NT(this.environmentInjector,this.configLoader,this.rootComponentType,e.config,this.urlSerializer,this.paramsInheritanceStrategy,a.signal),Ut(p=>{r.targetSnapshot=p.targetSnapshot,r.urlAfterRedirects=p.urlAfterRedirects,this.currentNavigation.update(f=>(f.finalUrl=p.urlAfterRedirects,f)),this.events.next(new gs)}),Pt(p=>Pe(r.routesRecognizeHandler.deferredHandle??Y(void 0)).pipe(_e(()=>p))),Ut(()=>{let p=new hd(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(p)}));if(d&&this.urlHandlingStrategy.shouldProcessUrl(l.currentRawUrl)){let{id:p,extractedUrl:f,source:h,restoredState:w,extras:I}=l,P=new wo(p,this.urlSerializer.serialize(f),h,w);this.events.next(P);let $=Hx(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=r=R(g({},l),{targetSnapshot:$,urlAfterRedirects:f,extras:R(g({},I),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(Je=>(Je.finalUrl=f,Je)),Y(r)}else return this.events.next(new Er(l.id,this.urlSerializer.serialize(l.extractedUrl),"",pd.IgnoredByUrlHandlingStrategy)),l.resolve(!1),qe}),_e(l=>{let c=new Kp(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);return this.events.next(c),this.currentTransition=r=R(g({},l),{guards:GN(l.targetSnapshot,l.currentSnapshot,this.rootContexts)}),r}),nT(l=>this.events.next(l)),Pt(l=>{if(r.guardsResult=l.guardsResult,l.guardsResult&&typeof l.guardsResult!="boolean")throw ms(this.urlSerializer,l.guardsResult);let c=new Zp(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot,!!l.guardsResult);if(this.events.next(c),!s())return qe;if(!l.guardsResult)return this.cancelNavigationTransition(l,"",vt.GuardRejected),qe;if(l.guards.canActivateChecks.length===0)return Y(l);let d=new Qp(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);if(this.events.next(d),!s())return qe;let m=!1;return Y(l).pipe(TT(this.paramsInheritanceStrategy),Ut({next:()=>{m=!0;let p=new Xp(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects),l.targetSnapshot);this.events.next(p)},complete:()=>{m||this.cancelNavigationTransition(l,"",vt.NoDataFromResolver)}}))}),dh(l=>{let c=m=>{let p=[];if(m.routeConfig?._loadedComponent)m.component=m.routeConfig?._loadedComponent;else if(m.routeConfig?.loadComponent){let f=m._environmentInjector;p.push(this.configLoader.loadComponent(f,m.routeConfig).then(h=>{m.component=h}))}for(let f of m.children)p.push(...c(f));return p},d=c(l.targetSnapshot.root);return d.length===0?Y(l):Pe(Promise.all(d).then(()=>l))}),Pt(l=>{let{newlyCreatedRoutes:c,state:d}=zN(e.routeReuseStrategy,l.targetSnapshot,l.currentRouterState);return this.currentTransition=r=l=R(g({},l),{targetRouterState:d,newlyCreatedRoutes:c}),this.currentNavigation.update(m=>(m.targetRouterState=d,m)),Y(l)}),this.routerResourcesFeature?.setupAndRunResources(a.signal)??(l=>l),dh(()=>this.afterPreactivation()),Pt(()=>{let{currentSnapshot:l,targetSnapshot:c}=r,d=this.createViewTransition?.(this.environmentInjector,l.root,c.root,r.hasUAVisualTransition);return d?Pe(d).pipe(_e(()=>r)):Y(r)}),Ft(1),Pt(l=>{i=!1,this.events.next(new Do);let c=r.beforeActivateHandler.deferredHandle;return c?Pe(c.then(()=>l)):Y(l)}),Ut(l=>{new oh(e.routeReuseStrategy,r.targetRouterState,r.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),l.newlyCreatedRoutes?.clear(),s()&&(mw(l.targetRouterState),o=!0,this.currentNavigation.update(c=>(c.abort=VT,c)),this.lastSuccessfulNavigation.set(Ie(this.currentNavigation)),this.events.next(new Sr(l.id,this.urlSerializer.serialize(l.extractedUrl),this.urlSerializer.serialize(l.urlAfterRedirects))),this.titleStrategy?.updateTitle(l.targetRouterState.snapshot),l.resolve(!0))}),Ue(Xx(a.signal).pipe(Ye(()=>!o&&i),Ut(()=>{this.cancelNavigationTransition(r,a.signal.reason+"",vt.Aborted)}))),Ut({complete:()=>{o=!0}}),Ue(this.transitionAbortWithErrorSubject.pipe(Ut(l=>{throw l}))),Uu(()=>{a.abort(),o||this.cancelNavigationTransition(r,"",vt.SupersededByNewNavigation),this.currentTransition?.id===r.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),fa(l=>{if(o=!0,Ex(r),this.destroyed)return r.resolve(!1),qe;if(l instanceof Eo&&(l=ms(this.urlSerializer,l)),Zx(l))this.events.next(new vn(r.id,this.urlSerializer.serialize(r.extractedUrl),l.message,l.cancellationCode)),$N(l)?this.events.next(new So(l.url,l.navigationBehaviorOptions)):r.resolve(!1);else{let c=new Co(r.id,this.urlSerializer.serialize(r.extractedUrl),l,r.targetSnapshot??void 0);try{let d=st(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(d instanceof Eo){let{message:m,cancellationCode:p}=ms(this.urlSerializer,d);this.events.next(new vn(r.id,this.urlSerializer.serialize(r.extractedUrl),m,p)),this.events.next(new So(d.redirectTo,d.navigationBehaviorOptions))}else throw this.events.next(c),l}catch(d){this.options.resolveNavigationPromiseOnError?r.resolve(!1):r.reject(d)}}return qe}))}))}cancelNavigationTransition(e,r,i){Ex(e);let o=new vn(e.id,this.urlSerializer.serialize(e.extractedUrl),r,i);this.events.next(o),e.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let e=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),r=Ie(this.currentNavigation),i=r?.targetBrowserUrl??r?.extractedUrl;return e.toString()!==i?.toString()&&!r?.extras.skipLocationChange}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function jT(t){return t!==us}function Ex(t){for(let n of t.newlyCreatedRoutes??[])n._localInjector?.destroy(),n._localInjector=void 0;mw(t.targetRouterState)}function mw(t){if(!t)return;let n=e=>{e.value.pending?.set(!1),e.children.forEach(n)};n(t._root)}var fw=new b("");var BT=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:()=>u(zT)})}return t})(),uh=class{shouldDetach(n){return!1}store(n,e){}shouldAttach(n){return!1}retrieve(n){return null}shouldReuseRoute(n,e){return n.routeConfig===e.routeConfig}shouldDestroyInjector(n){return!0}},zT=(()=>{class t extends uh{static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),gh=(()=>{class t{urlSerializer=u(bd);options=u(_d,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=u(Dr);urlHandlingStrategy=u(hh);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new _n;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:e,initialUrl:r,targetBrowserUrl:i}){let o=e!==void 0?this.urlHandlingStrategy.merge(e,r):r,a=i??o;return a instanceof _n?this.urlSerializer.serialize(a):a}routerUrlState(e){return e?.targetBrowserUrl===void 0||e?.finalUrl===void 0?{}:{\u0275routerUrl:this.urlSerializer.serialize(e.finalUrl)}}commitTransition({targetRouterState:e,finalUrl:r,initialUrl:i}){r&&e?(this.currentUrlTree=r,this.rawUrlTree=this.urlHandlingStrategy.merge(r,i),this.routerState=e):this.rawUrlTree=i}routerState=Hx(null,u(Le));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:()=>u(HT)})}return t})(),HT=(()=>{class t extends gh{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(e){return this.location.subscribe(r=>{r.type==="popstate"&&setTimeout(()=>{e(r.url,r.state,"popstate",{replaceUrl:!0},r.hasUAVisualTransition)})})}handleRouterEvent(e,r){e instanceof wo?this.updateStateMemento():e instanceof Er?this.commitTransition(r):e instanceof hd?this.urlUpdateStrategy==="eager"&&(r.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(r),r)):e instanceof Do?(this.commitTransition(r),this.urlUpdateStrategy==="deferred"&&!r.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(r),r)):e instanceof vn?qx(e)?e.code===vt.Redirect&&this.routerState===r.targetRouterState&&this.resetInternalState(r):this.restoreHistory(r):e instanceof Co?this.restoreHistory(r,!0):e instanceof Sr&&(this.lastSuccessfulId=e.id,this.currentPageId=this.browserPageId)}setBrowserUrl(e,r){let{extras:i,id:o}=r,{replaceUrl:a,state:s}=i;if(this.location.isCurrentPathEqualTo(e)||a){let l=this.browserPageId,c=g(g({},s),this.generateNgRouterState(o,l,r));this.location.replaceState(e,"",c)}else{let l=g(g({},s),this.generateNgRouterState(o,this.browserPageId+1,r));this.location.go(e,"",l)}}restoreHistory(e,r=!1){if(this.canceledNavigationResolution==="computed"){let i=this.browserPageId,o=this.currentPageId-i;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===e.finalUrl&&o===0&&(this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(r&&this.resetInternalState(e),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:e}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,e??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(e,r,i){return this.canceledNavigationResolution==="computed"?g({navigationId:e,\u0275routerPageId:r},this.routerUrlState(i)):g({navigationId:e},this.routerUrlState(i))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function pw(t,n){t.events.pipe(Ye(e=>e instanceof Sr||e instanceof vn||e instanceof Co||e instanceof Er),_e(e=>e instanceof Sr||e instanceof Er?0:(e instanceof vn?e.code===vt.Redirect||e.code===vt.SupersededByNewNavigation:!1)?2:1),Ye(e=>e!==2),Ft(1)).subscribe(()=>{n()})}var bh=(()=>{class t{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=u(sp);stateManager=u(gh);options=u(_d,{optional:!0})||{};pendingTasks=u(vr);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=u(uw);urlSerializer=u(bd);location=u(Dr);urlHandlingStrategy=u(hh);injector=u(Le);_events=new E;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=u(BT);injectorCleanup=u(fw,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=u(xd,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!u(vd,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:e=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new ue;subscribeToNavigationEvents(){let e=this.navigationTransitions.events.subscribe(r=>{try{let i=this.navigationTransitions.currentTransition,o=Ie(this.navigationTransitions.currentNavigation);if(i!==null&&o!==null){if(this.stateManager.handleRouterEvent(r,o),r instanceof vn&&r.code!==vt.Redirect&&r.code!==vt.SupersededByNewNavigation)this.navigated=!0;else if(r instanceof Sr)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(r instanceof So){let a=r.navigationBehaviorOptions,s=this.urlHandlingStrategy.merge(r.url,i.currentRawUrl),l=g({scroll:i.extras.scroll,browserUrl:i.extras.browserUrl,info:i.extras.info,skipLocationChange:i.extras.skipLocationChange,replaceUrl:i.extras.replaceUrl||this.urlUpdateStrategy==="eager"||jT(i.source)},a);this.scheduleNavigation(s,us,null,l,i.hasUAVisualTransition,{resolve:i.resolve,reject:i.reject,promise:i.promise})}}BN(r)&&this._events.next(r)}catch(i){this.navigationTransitions.transitionAbortWithErrorSubject.next(i)}});this.eventsSubscription.add(e)}resetRootComponentType(e){this.routerState.root.component=e,this.navigationTransitions.rootComponentType=e}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),us,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((e,r,i,o,a)=>{this.navigateToSyncWithBrowser(e,i,r,o,a)})}navigateToSyncWithBrowser(e,r,i,o,a){let s=i?.navigationId?i:null,l=i?.\u0275routerUrl??e;if(i?.\u0275routerUrl&&(o=R(g({},o),{browserUrl:e})),i){let d=g({},i);delete d.navigationId,delete d.\u0275routerPageId,delete d.\u0275routerUrl,Object.keys(d).length!==0&&(o.state=d)}let c=this.parseUrl(l);this.scheduleNavigation(c,r,s,o,a).catch(d=>{this.disposed||this.injector.get(mn)(d)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return Ie(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(e){this.config=e.map(ph),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(e,r={}){let{relativeTo:i,queryParams:o,fragment:a,queryParamsHandling:s,preserveFragment:l}=r,c=l?this.currentUrlTree.fragment:a,d=null;switch(s??this.options.defaultQueryParamsHandling){case"merge":d=g(g({},this.currentUrlTree.queryParams),o);break;case"preserve":d=this.currentUrlTree.queryParams;break;default:d=o||null}d!==null&&(d=this.removeEmptyProps(d));let m;try{let p=i?i.snapshot:this.routerState.snapshot.root;m=Vx(p)}catch{(typeof e[0]!="string"||e[0][0]!=="/")&&(e=[]),m=this.currentUrlTree.root}return jx(m,e,d,c??null,this.urlSerializer)}navigateByUrl(e,r={skipLocationChange:!1}){let i=_o(e)?e:this.parseUrl(e),o=this.urlHandlingStrategy.merge(i,this.rawUrlTree);return this.scheduleNavigation(o,us,null,r)}navigate(e,r={skipLocationChange:!1}){return UT(e),this.navigateByUrl(this.createUrlTree(e,r),r)}serializeUrl(e){return this.urlSerializer.serialize(e)}parseUrl(e){try{return this.urlSerializer.parse(e)}catch{return this.console.warn(Qr(4018,!1)),this.urlSerializer.parse("/")}}isActive(e,r){let i;if(r===!0?i=g({},Nx):r===!1?i=g({},sd):i=g(g({},sd),r),_o(e))return yx(this.currentUrlTree,e,i);let o=this.parseUrl(e);return yx(this.currentUrlTree,o,i)}removeEmptyProps(e){return Object.entries(e).reduce((r,[i,o])=>(o!=null&&(r[i]=o),r),{})}scheduleNavigation(e,r,i,o,a,s){if(this.disposed)return Promise.resolve(!1);let l,c,d;s?(l=s.resolve,c=s.reject,d=s.promise):d=new Promise((p,f)=>{l=p,c=f});let m=this.pendingTasks.add();return pw(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(m))}),this.navigationTransitions.handleNavigationRequest({source:r,restoredState:i,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:e,extras:o,hasUAVisualTransition:a,resolve:l,reject:c,promise:d,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),d.catch(Promise.reject.bind(Promise))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function UT(t){for(let n=0;n<t.length;n++)if(t[n]==null)throw new D(4008,!1)}var $T=new b("");function vh(t,...n){return fr([{provide:xd,multi:!0,useValue:t},{provide:Ci,useFactory:GT},{provide:zc,multi:!0,useFactory:WT},n.map(e=>e.\u0275providers)])}function GT(){return u(bh).routerState.root}function WT(){let t=u(J);return n=>{let e=t.get(Kt);if(n!==e.components[0])return;let r=t.get(bh),i=t.get(qT);t.get(YT)===1&&r.initialNavigation(),t.get(KT,null,{optional:!0})?.setUpPreloading(),t.get($T,null,{optional:!0})?.init(),r.resetRootComponentType(e.componentTypes[0]),i.closed||(i.next(),i.complete(),i.unsubscribe())}}var qT=new b("",{factory:()=>new E}),YT=new b("",{factory:()=>1});var KT=new b("");var hw=[];var gw={providers:[$m(),vh(hw)]};var QT=[{name:"Pepperoni",price:3.5},{name:"Sausage",price:4.25},{name:"Extra Cheese",price:.99}],wd=class t{getAvailablePizzaToppings=()=>QT.map(e=>R(g({},e),{checked:!1}));static \u0275fac=function(e){return new(e||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})};function xs(t){return t.buttons===0||t.detail===0}function ws(t){let n=t.touches&&t.touches[0]||t.changedTouches&&t.changedTouches[0];return!!n&&n.identifier===-1&&(n.radiusX==null||n.radiusX===1)&&(n.radiusY==null||n.radiusY===1)}var yh;function bw(){if(yh==null){let t=typeof document<"u"?document.head:null;yh=!!(t&&(t.createShadowRoot||t.attachShadow))}return yh}function _h(t){if(bw()){let n=t.getRootNode?t.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&n instanceof ShadowRoot)return n}return null}function Mo(){let t=typeof document<"u"&&document?document.activeElement:null;for(;t&&t.shadowRoot;){let n=t.shadowRoot.activeElement;if(n===t)break;t=n}return t}function Nt(t){if(t.composedPath)try{return t.composedPath()[0]}catch{}return t.target}var xh;try{xh=typeof Intl<"u"&&Intl.v8BreakIterator}catch{xh=!1}var he=(()=>{class t{_platformId=u(li);isBrowser=this._platformId?dx(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||xh)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var Cs;function vw(){if(Cs==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Cs=!0}))}finally{Cs=Cs||!1}return Cs}function No(t){return vw()?t:!!t.capture}function To(t,n=0){return yw(t)?Number(t):arguments.length===2?n:0}function yw(t){return!isNaN(parseFloat(t))&&!isNaN(Number(t))}function Bt(t){return t instanceof V?t.nativeElement:t}var _w=new b("cdk-input-modality-detector-options"),xw={ignoreKeys:[18,17,224,91,16]},ww=650,wh={passive:!0,capture:!0},Cw=(()=>{class t{_platform=u(he);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new tt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(r=>r===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=Nt(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<ww||(this._modality.next(xs(e)?"keyboard":"mouse"),this._mostRecentTarget=Nt(e))};_onTouchstart=e=>{if(ws(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=Nt(e)};constructor(){let e=u(T),r=u(G),i=u(_w,{optional:!0});if(this._options=g(g({},xw),i),this.modalityDetected=this._modality.pipe(pa(1)),this.modalityChanged=this.modalityDetected.pipe(Ml()),this._platform.isBrowser){let o=u(Xe).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(r,"keydown",this._onKeydown,wh),o.listen(r,"mousedown",this._onMousedown,wh),o.listen(r,"touchstart",this._onTouchstart,wh)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),Ds=(function(t){return t[t.IMMEDIATE=0]="IMMEDIATE",t[t.EVENTUAL=1]="EVENTUAL",t})(Ds||{}),Dw=new b("cdk-focus-monitor-default-options"),Cd=No({passive:!0,capture:!0}),en=(()=>{class t{_ngZone=u(T);_platform=u(he);_inputModalityDetector=u(Cw);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(G);_stopInputModalityDetector=new E;constructor(){let e=u(Dw,{optional:!0});this._detectionMode=e?.detectionMode||Ds.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let r=Nt(e);for(let i=r;i;i=i.parentElement)e.type==="focus"?this._onFocus(e,i):this._onBlur(e,i)};monitor(e,r=!1){let i=Bt(e);if(!this._platform.isBrowser||i.nodeType!==1)return Y();let o=_h(i)||this._document,a=this._elementInfo.get(i);if(a)return r&&(a.checkChildren=!0),a.subject;let s={checkChildren:r,subject:new E,rootNode:o};return this._elementInfo.set(i,s),this._registerGlobalListeners(s),s.subject}stopMonitoring(e){let r=Bt(e),i=this._elementInfo.get(r);i&&(i.subject.complete(),this._setClasses(r),this._elementInfo.delete(r),this._removeGlobalListeners(i))}focusVia(e,r,i){let o=Bt(e),a=this._document.activeElement;o===a?this._getClosestElementsInfo(o).forEach(([s,l])=>this._originChanged(s,r,l)):(this._setOrigin(r),typeof o.focus=="function"&&o.focus(i))}ngOnDestroy(){this._elementInfo.forEach((e,r)=>this.stopMonitoring(r))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Ds.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,r){e.classList.toggle("cdk-focused",!!r),e.classList.toggle("cdk-touch-focused",r==="touch"),e.classList.toggle("cdk-keyboard-focused",r==="keyboard"),e.classList.toggle("cdk-mouse-focused",r==="mouse"),e.classList.toggle("cdk-program-focused",r==="program")}_setOrigin(e,r=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&r,this._detectionMode===Ds.IMMEDIATE){clearTimeout(this._originTimeoutId);let i=this._originFromTouchInteraction?ww:1;this._originTimeoutId=setTimeout(()=>this._origin=null,i)}})}_onFocus(e,r){let i=this._elementInfo.get(r),o=Nt(e);!i||!i.checkChildren&&r!==o||this._originChanged(r,this._getFocusOrigin(o),i)}_onBlur(e,r){let i=this._elementInfo.get(r);!i||i.checkChildren&&e.relatedTarget instanceof Node&&r.contains(e.relatedTarget)||(this._setClasses(r),this._emitOrigin(i,null))}_emitOrigin(e,r){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(r))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let r=e.rootNode,i=this._rootNodeFocusListenerCount.get(r)||0;i||this._ngZone.runOutsideAngular(()=>{r.addEventListener("focus",this._rootNodeFocusAndBlurListener,Cd),r.addEventListener("blur",this._rootNodeFocusAndBlurListener,Cd)}),this._rootNodeFocusListenerCount.set(r,i+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ue(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let r=e.rootNode;if(this._rootNodeFocusListenerCount.has(r)){let i=this._rootNodeFocusListenerCount.get(r);i>1?this._rootNodeFocusListenerCount.set(r,i-1):(r.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Cd),r.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Cd),this._rootNodeFocusListenerCount.delete(r))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,r,i){this._setClasses(e,r),this._emitOrigin(i,r),this._lastFocusOrigin=r}_getClosestElementsInfo(e){let r=[];return this._elementInfo.forEach((i,o)=>{(o===e||i.checkChildren&&o.contains(e))&&r.push([o,i])}),r}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:r,mostRecentModality:i}=this._inputModalityDetector;if(i!=="mouse"||!r||r===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let a=0;a<o.length;a++)if(o[a].contains(r))return!0}return!1}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),Ch=(()=>{class t{_elementRef=u(V);_focusMonitor=u(en);_monitorSubscription;_focusOrigin=null;cdkFocusChange=new C;get focusOrigin(){return this._focusOrigin}ngAfterViewInit(){let e=this._elementRef.nativeElement;this._monitorSubscription=this._focusMonitor.monitor(e,e.nodeType===1&&e.hasAttribute("cdkMonitorSubtreeFocus")).subscribe(r=>{this._focusOrigin=r,this.cdkFocusChange.emit(r)})}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef),this._monitorSubscription?.unsubscribe()}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["","cdkMonitorElementFocus",""],["","cdkMonitorSubtreeFocus",""]],outputs:{cdkFocusChange:"cdkFocusChange"},exportAs:["cdkMonitorFocus"]})}return t})();var Dd=new WeakMap,Me=(()=>{class t{_appRef;_injector=u(J);_environmentInjector=u(Le);load(e){let r=this._appRef=this._appRef||this._injector.get(Kt),i=Dd.get(r);i||(i={loaders:new Set,refs:[]},Dd.set(r,i),r.onDestroy(()=>{Dd.get(r)?.refs.forEach(o=>o.destroy()),Dd.delete(r)})),i.loaders.has(e)||(i.loaders.add(e),i.refs.push(Wc(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var kr=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(r,i){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2})}return t})();function Ao(t){return Array.isArray(t)?t:[t]}var Sw=new Set,Si,Ro=(()=>{class t{_platform=u(he);_nonce=u(ci,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):JT}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&XT(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function XT(t,n){if(!Sw.has(t))try{Si||(Si=document.createElement("style"),n&&Si.setAttribute("nonce",n),Si.setAttribute("type","text/css"),document.head.appendChild(Si)),Si.sheet&&(Si.sheet.insertRule(`@media ${t.replace(/[{}]/g,"")} {body{ }}`,0),Sw.add(t))}catch(e){console.error(e)}}function JT(t){return{matches:t==="all"||t==="",media:t,addListener:()=>{},removeListener:()=>{}}}var Dh=(()=>{class t{_mediaMatcher=u(Ro);_zone=u(T);_queries=new Map;_destroySubject=new E;ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return Ew(Ao(e)).some(i=>this._registerQuery(i).mql.matches)}observe(e){let i=Ew(Ao(e)).map(a=>this._registerQuery(a).observable),o=ua(i);return o=cr(o.pipe(Ft(1)),o.pipe(pa(1),Hu(0))),o.pipe(_e(a=>{let s={matches:!1,breakpoints:{}};return a.forEach(({matches:l,query:c})=>{s.matches=s.matches||l,s.breakpoints[c]=l}),s}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let r=this._mediaMatcher.matchMedia(e),o={observable:new q(a=>{let s=l=>this._zone.run(()=>a.next(l));return r.addListener(s),()=>{r.removeListener(s)}}).pipe(Ht(r),_e(({matches:a})=>({query:e,matches:a})),Ue(this._destroySubject)),mql:r};return this._queries.set(e,o),o}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function Ew(t){return t.map(n=>n.split(",")).reduce((n,e)=>n.concat(e)).map(n=>n.trim())}var eA=(()=>{class t{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var Sd=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({providers:[eA]})}return t})();var tA=(()=>{class t{_platform=u(he);isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return rA(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let r=nA(uA(e));if(r&&(kw(r)===-1||!this.isVisible(r)))return!1;let i=e.nodeName.toLowerCase(),o=kw(e);return e.hasAttribute("contenteditable")?o!==-1:i==="iframe"||i==="object"||this._platform.WEBKIT&&this._platform.IOS&&!cA(e)?!1:i==="audio"?e.hasAttribute("controls")?o!==-1:!1:i==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,r){return dA(e)&&!this.isDisabled(e)&&(r?.ignoreInert||!e.closest?.("[inert]"))&&(r?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function nA(t){try{return t.frameElement}catch{return null}}function rA(t){return!!(t.offsetWidth||t.offsetHeight||typeof t.getClientRects=="function"&&t.getClientRects().length)}function iA(t){let n=t.nodeName.toLowerCase();return n==="input"||n==="select"||n==="button"||n==="textarea"}function oA(t){return sA(t)&&t.type=="hidden"}function aA(t){return lA(t)&&t.hasAttribute("href")}function sA(t){return t.nodeName.toLowerCase()=="input"}function lA(t){return t.nodeName.toLowerCase()=="a"}function Nw(t){if(!t.hasAttribute("tabindex")||t.tabIndex===void 0)return!1;let n=t.getAttribute("tabindex");return!!(n&&!isNaN(parseInt(n,10)))}function kw(t){if(!Nw(t))return null;let n=parseInt(t.getAttribute("tabindex")||"",10);return isNaN(n)?-1:n}function cA(t){let n=t.nodeName.toLowerCase(),e=n==="input"&&t.type;return e==="text"||e==="password"||n==="select"||n==="textarea"}function dA(t){return oA(t)?!1:iA(t)||aA(t)||t.hasAttribute("contenteditable")||Nw(t)}function uA(t){return t.ownerDocument&&t.ownerDocument.defaultView||window}var Eh=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>{!this.focusLastTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};endAnchorListener=()=>{!this.focusFirstTabbableElement()&&this._checker.isFocusable(this._element)&&this._element.focus()};get enabled(){return this._enabled}set enabled(n){this._enabled=n,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_enabled=!0;constructor(n,e,r,i,o=!1,a){this._element=n,this._checker=e,this._ngZone=r,this._document=i,this._injector=a,o||this.attachAnchors()}destroy(){let n=this._startAnchor,e=this._endAnchor;n&&(n.removeEventListener("focus",this.startAnchorListener),n.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(n)))})}focusFirstTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(n)))})}focusLastTabbableElementWhenReady(n){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(n)))})}_getRegionBoundary(n){let e=this._element.querySelectorAll(`[cdk-focus-region-${n}], [cdkFocusRegion${n}], [cdk-focus-${n}]`);return n=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(n){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let r=this._getFirstTabbableElement(e);return r?.focus(n),!!r}return e.focus(n),!0}return this.focusFirstTabbableElement(n)}focusFirstTabbableElement(n){let e=this._getRegionBoundary("start");return e&&e.focus(n),!!e}focusLastTabbableElement(n){let e=this._getRegionBoundary("end");return e&&e.focus(n),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let r=0;r<e.length;r++){let i=e[r].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[r]):null;if(i)return i}return null}_getLastTabbableElement(n){if(this._checker.isFocusable(n)&&this._checker.isTabbable(n))return n;let e=n.children;for(let r=e.length-1;r>=0;r--){let i=e[r].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[r]):null;if(i)return i}return null}_createAnchor(){let n=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,n),n.classList.add("cdk-visually-hidden"),n.classList.add("cdk-focus-trap-anchor"),n.setAttribute("aria-hidden","true"),n}_toggleAnchorTabIndex(n,e){n?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(n){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(n,this._startAnchor),this._toggleAnchorTabIndex(n,this._endAnchor))}_executeOnStable(n){it(n,{injector:this._injector})}},Tw=(()=>{class t{_checker=u(tA);_ngZone=u(T);_document=u(G);_injector=u(J);constructor(){u(Me).load(kr)}create(e,r=!1){return new Eh(e,this._checker,this._ngZone,this._document,r,this._injector)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),kh=(()=>{class t{_elementRef=u(V);_focusTrapFactory=u(Tw);focusTrap=void 0;_previouslyFocusedElement=null;get enabled(){return this.focusTrap?.enabled||!1}set enabled(e){this.focusTrap&&(this.focusTrap.enabled=e)}autoCapture=!1;constructor(){u(he).isBrowser&&(this.focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement,!0))}ngOnDestroy(){this.focusTrap?.destroy(),this._previouslyFocusedElement&&(this._previouslyFocusedElement.focus(),this._previouslyFocusedElement=null)}ngAfterContentInit(){this.focusTrap?.attachAnchors(),this.autoCapture&&this._captureFocus()}ngDoCheck(){this.focusTrap&&!this.focusTrap.hasAttached()&&this.focusTrap.attachAnchors()}ngOnChanges(e){let r=e.autoCapture;r&&!r.firstChange&&this.autoCapture&&this.focusTrap?.hasAttached()&&this._captureFocus()}_captureFocus(){this._previouslyFocusedElement=Mo(),this.focusTrap?.focusInitialElementWhenReady()}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["","cdkTrapFocus",""]],inputs:{enabled:[2,"cdkTrapFocus","enabled",N],autoCapture:[2,"cdkTrapFocusAutoCapture","autoCapture",N]},exportAs:["cdkTrapFocus"],features:[Ae]})}return t})();var Ir=(function(t){return t[t.NONE=0]="NONE",t[t.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",t[t.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",t})(Ir||{}),Iw="cdk-high-contrast-black-on-white",Mw="cdk-high-contrast-white-on-black",Sh="cdk-high-contrast-active",Aw=(()=>{class t{_platform=u(he);_hasCheckedHighContrastMode=!1;_document=u(G);_breakpointSubscription;constructor(){this._breakpointSubscription=u(Dh).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return Ir.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let r=this._document.defaultView||window,i=r&&r.getComputedStyle?r.getComputedStyle(e):null,o=(i&&i.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return Ir.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return Ir.BLACK_ON_WHITE}return Ir.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(Sh,Iw,Mw),this._hasCheckedHighContrastMode=!0;let r=this.getHighContrastMode();r===Ir.BLACK_ON_WHITE?e.add(Sh,Iw):r===Ir.WHITE_ON_BLACK&&e.add(Sh,Mw)}}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),Ih=(()=>{class t{constructor(){u(Aw)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Sd]})}return t})();function xn(t,...n){return n.length?n.some(e=>t[e]):t.altKey||t.shiftKey||t.ctrlKey||t.metaKey}var Rw=new Map,je=class t{_appId=u(Zn);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(n,e=!1){this._appId!=="ng"&&(n+=this._appId);let r=Rw.get(n);return r===void 0?r=0:r++,Rw.set(n,r),`${n}${e?t._infix+"-":""}${r}`}static \u0275fac=function(e){return new(e||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})};var Fw=" ";function mA(t,n,e){let r=kd(t,n);e=e.trim(),!r.some(i=>i.trim()===e)&&(r.push(e),t.setAttribute(n,r.join(Fw)))}function fA(t,n,e){let r=kd(t,n);e=e.trim();let i=r.filter(o=>o!==e);i.length?t.setAttribute(n,i.join(Fw)):t.removeAttribute(n)}function kd(t,n){return t.getAttribute(n)?.match(/\S+/g)??[]}var Pw="cdk-describedby-message",Ed="cdk-describedby-host",Nh=0,Lw=(()=>{class t{_platform=u(he);_document=u(G);_messageRegistry=new Map;_messagesContainer=null;_id=`${Nh++}`;constructor(){u(Me).load(kr),this._id=u(Zn)+"-"+Nh++}describe(e,r,i){if(!this._canBeDescribed(e,r))return;let o=Mh(r,i);typeof r!="string"?(Ow(r,this._id),this._messageRegistry.set(o,{messageElement:r,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(r,i),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,r,i){if(!r||!this._isElementNode(e))return;let o=Mh(r,i);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof r=="string"){let a=this._messageRegistry.get(o);a&&a.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${Ed}="${this._id}"]`);for(let r=0;r<e.length;r++)this._removeCdkDescribedByReferenceIds(e[r]),e[r].removeAttribute(Ed);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,r){let i=this._document.createElement("div");Ow(i,this._id),i.textContent=e,r&&i.setAttribute("role",r),this._createMessagesContainer(),this._messagesContainer.appendChild(i),this._messageRegistry.set(Mh(e,r),{messageElement:i,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",r=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<r.length;o++)r[o].remove();let i=this._document.createElement("div");i.style.visibility="hidden",i.classList.add(e),i.classList.add("cdk-visually-hidden"),this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._messagesContainer=i}_removeCdkDescribedByReferenceIds(e){let r=kd(e,"aria-describedby").filter(i=>i.indexOf(Pw)!=0);e.setAttribute("aria-describedby",r.join(" "))}_addMessageReference(e,r){let i=this._messageRegistry.get(r);mA(e,"aria-describedby",i.messageElement.id),e.setAttribute(Ed,this._id),i.referenceCount++}_removeMessageReference(e,r){let i=this._messageRegistry.get(r);i.referenceCount--,fA(e,"aria-describedby",i.messageElement.id),e.removeAttribute(Ed)}_isElementDescribedByMessage(e,r){let i=kd(e,"aria-describedby"),o=this._messageRegistry.get(r),a=o&&o.messageElement.id;return!!a&&i.indexOf(a)!=-1}_canBeDescribed(e,r){if(!this._isElementNode(e))return!1;if(r&&typeof r=="object")return!0;let i=r==null?"":`${r}`.trim(),o=e.getAttribute("aria-label");return i?!o||o.trim()!==i:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function Mh(t,n){return typeof t=="string"?`${n||""}/${t}`:t}function Ow(t,n){t.id||(t.id=`${Pw}-${n}-${Nh++}`)}var Ei;function Vw(){if(Ei==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Ei=!1,Ei;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Ei=!0;else{let t=Element.prototype.scrollTo;t?Ei=!/\{\s*\[native code\]\s*\}/.test(t.toString()):Ei=!1}}return Ei}function Th(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var Oo,jw=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function Ah(){if(Oo)return Oo;if(typeof document!="object"||!document)return Oo=new Set(jw),Oo;let t=document.createElement("input");return Oo=new Set(jw.filter(n=>(t.setAttribute("type",n),t.type===n))),Oo}var pA=new b("MATERIAL_ANIMATIONS"),Bw=null;function hA(){return u(pA,{optional:!0})?.animationsDisabled||u(Ra,{optional:!0})==="NoopAnimations"?"di-disabled":(Bw??=u(Ro).matchMedia("(prefers-reduced-motion)").matches,Bw?"reduced-motion":"enabled")}function Be(){return hA()!=="enabled"}function Qe(t){return t==null?"":typeof t=="string"?t:`${t}px`}function tr(t){return t!=null&&`${t}`!="false"}function zw(t,n=/\s+/){let e=[];if(t!=null){let r=Array.isArray(t)?t:`${t}`.split(n);for(let i of r){let o=`${i}`.trim();o&&e.push(o)}}return e}var tn=(function(t){return t[t.FADING_IN=0]="FADING_IN",t[t.VISIBLE=1]="VISIBLE",t[t.FADING_OUT=2]="FADING_OUT",t[t.HIDDEN=3]="HIDDEN",t})(tn||{}),Rh=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=tn.HIDDEN;constructor(n,e,r,i=!1){this._renderer=n,this.element=e,this.config=r,this._animationForciblyDisabledThroughCss=i}fadeOut(){this._renderer.fadeOutRipple(this)}},Hw=No({passive:!0,capture:!0}),Oh=class{_events=new Map;addHandler(n,e,r,i){let o=this._events.get(e);if(o){let a=o.get(r);a?a.add(i):o.set(r,new Set([i]))}else this._events.set(e,new Map([[r,new Set([i])]])),n.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,Hw)})}removeHandler(n,e,r){let i=this._events.get(n);if(!i)return;let o=i.get(e);o&&(o.delete(r),o.size===0&&i.delete(e),i.size===0&&(this._events.delete(n),document.removeEventListener(n,this._delegateEventHandler,Hw)))}_delegateEventHandler=n=>{let e=Nt(n);e&&this._events.get(n.type)?.forEach((r,i)=>{(i===e||i.contains(e))&&r.forEach(o=>o.handleEvent(n))})}},Ss={enterDuration:225,exitDuration:150},gA=800,Uw=No({passive:!0,capture:!0}),$w=["mousedown","touchstart"],Gw=["mouseup","mouseleave","touchend","touchcancel"],bA=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--%NS%mat-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2})}return t})(),Es=class t{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new Oh;constructor(n,e,r,i,o){this._target=n,this._ngZone=e,this._platform=i,i.isBrowser&&(this._containerElement=Bt(r)),o&&o.get(Me).load(bA)}fadeInRipple(n,e,r={}){let i=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=g(g({},Ss),r.animation);r.centered&&(n=i.left+i.width/2,e=i.top+i.height/2);let a=r.radius||vA(n,e,i),s=n-i.left,l=e-i.top,c=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${s-a}px`,d.style.top=`${l-a}px`,d.style.height=`${a*2}px`,d.style.width=`${a*2}px`,r.color!=null&&(d.style.backgroundColor=r.color),d.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(d);let m=window.getComputedStyle(d),p=m.transitionProperty,f=m.transitionDuration,h=p==="none"||f==="0s"||f==="0s, 0s"||i.width===0&&i.height===0,w=new Rh(this,d,r,h);d.style.transform="scale3d(1, 1, 1)",w.state=tn.FADING_IN,r.persistent||(this._mostRecentTransientRipple=w);let I=null;return!h&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let P=()=>{I&&(I.fallbackTimer=null),clearTimeout(Je),this._finishRippleTransition(w)},$=()=>this._destroyRipple(w),Je=setTimeout($,c+100);d.addEventListener("transitionend",P),d.addEventListener("transitioncancel",$),I={onTransitionEnd:P,onTransitionCancel:$,fallbackTimer:Je}}),this._activeRipples.set(w,I),(h||!c)&&this._finishRippleTransition(w),w}fadeOutRipple(n){if(n.state===tn.FADING_OUT||n.state===tn.HIDDEN)return;let e=n.element,r=g(g({},Ss),n.config.animation);e.style.transitionDuration=`${r.exitDuration}ms`,e.style.opacity="0",n.state=tn.FADING_OUT,(n._animationForciblyDisabledThroughCss||!r.exitDuration)&&this._finishRippleTransition(n)}fadeOutAll(){this._getActiveRipples().forEach(n=>n.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(n=>{n.config.persistent||n.fadeOut()})}setupTriggerEvents(n){let e=Bt(n);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,$w.forEach(r=>{t._eventManager.addHandler(this._ngZone,r,e,this)}))}handleEvent(n){n.type==="mousedown"?this._onMousedown(n):n.type==="touchstart"?this._onTouchStart(n):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Gw.forEach(e=>{this._triggerElement.addEventListener(e,this,Uw)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(n){n.state===tn.FADING_IN?this._startFadeOutTransition(n):n.state===tn.FADING_OUT&&this._destroyRipple(n)}_startFadeOutTransition(n){let e=n===this._mostRecentTransientRipple,{persistent:r}=n.config;n.state=tn.VISIBLE,!r&&(!e||!this._isPointerDown)&&n.fadeOut()}_destroyRipple(n){let e=this._activeRipples.get(n)??null;this._activeRipples.delete(n),this._activeRipples.size||(this._containerRect=null),n===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),n.state=tn.HIDDEN,e!==null&&(n.element.removeEventListener("transitionend",e.onTransitionEnd),n.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),n.element.remove()}_onMousedown(n){let e=xs(n),r=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+gA;!this._target.rippleDisabled&&!e&&!r&&(this._isPointerDown=!0,this.fadeInRipple(n.clientX,n.clientY,this._target.rippleConfig))}_onTouchStart(n){if(!this._target.rippleDisabled&&!ws(n)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=n.changedTouches;if(e)for(let r=0;r<e.length;r++)this.fadeInRipple(e[r].clientX,e[r].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(n=>{let e=n.state===tn.VISIBLE||n.config.terminateOnPointerUp&&n.state===tn.FADING_IN;!n.config.persistent&&e&&n.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let n=this._triggerElement;n&&($w.forEach(e=>t._eventManager.removeHandler(e,n,this)),this._pointerUpEventsRegistered&&(Gw.forEach(e=>n.removeEventListener(e,this,Uw)),this._pointerUpEventsRegistered=!1))}};function vA(t,n,e){let r=Math.max(Math.abs(t-e.left),Math.abs(t-e.right)),i=Math.max(Math.abs(n-e.top),Math.abs(n-e.bottom));return Math.sqrt(r*r+i*i)}var Fh=new b("mat-ripple-global-options"),Mr=(()=>{class t{_elementRef=u(V);_animationsDisabled=Be();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(T),r=u(he),i=u(Fh,{optional:!0}),o=u(J);this._globalOptions=i||{},this._rippleRenderer=new Es(this,e,this._elementRef,r,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:g(g(g({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,r=0,i){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,r,g(g({},this.rippleConfig),i)):this._rippleRenderer.fadeInRipple(0,0,g(g({},this.rippleConfig),e))}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(r,i){r&2&&U("mat-ripple-unbounded",i.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return t})();var yA={capture:!0},_A=["focus","mousedown","mouseenter","touchstart"],Ph="mat-ripple-loader-uninitialized",Lh="mat-ripple-loader-class-name",Ww="mat-ripple-loader-centered",Id="mat-ripple-loader-disabled",qw=(()=>{class t{_document=u(G);_animationsDisabled=Be();_globalRippleOptions=u(Fh,{optional:!0});_platform=u(he);_ngZone=u(T);_injector=u(J);_eventCleanups;_hosts=new Map;constructor(){let e=u(Xe).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>_A.map(r=>e.listen(this._document,r,this._onInteraction,yA)))}ngOnDestroy(){let e=this._hosts.keys();for(let r of e)this.destroyRipple(r);this._eventCleanups.forEach(r=>r())}configureRipple(e,r){e.setAttribute(Ph,this._globalRippleOptions?.namespace??""),(r.className||!e.hasAttribute(Lh))&&e.setAttribute(Lh,r.className||""),r.centered&&e.setAttribute(Ww,""),r.disabled&&e.setAttribute(Id,"")}setDisabled(e,r){let i=this._hosts.get(e);i?(i.target.rippleDisabled=r,!r&&!i.hasSetUpEvents&&(i.hasSetUpEvents=!0,i.renderer.setupTriggerEvents(e))):r?e.setAttribute(Id,""):e.removeAttribute(Id)}_onInteraction=e=>{let r=Nt(e);if(r instanceof HTMLElement){let i=r.closest(`[${Ph}="${this._globalRippleOptions?.namespace??""}"]`);i&&this._createRipple(i)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let r=this._document.createElement("span");r.classList.add("mat-ripple",e.getAttribute(Lh)),e.append(r);let i=this._globalRippleOptions,o=this._animationsDisabled?0:i?.animation?.enterDuration??Ss.enterDuration,a=this._animationsDisabled?0:i?.animation?.exitDuration??Ss.exitDuration,s={rippleDisabled:this._animationsDisabled||i?.disabled||e.hasAttribute(Id),rippleConfig:{centered:e.hasAttribute(Ww),terminateOnPointerUp:i?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:a}}},l=new Es(s,this._ngZone,r,this._platform,this._injector),c=!s.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:s,renderer:l,hasSetUpEvents:c}),e.removeAttribute(Ph)}destroyRipple(e){let r=this._hosts.get(e);r&&(r.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var nn=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["structural-styles"]],decls:0,vars:0,template:function(r,i){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--%NS%mat-focus-indicator-display, none);
  border-width: var(--%NS%mat-focus-indicator-border-width, 3px);
  border-style: var(--%NS%mat-focus-indicator-border-style, solid);
  border-color: var(--%NS%mat-focus-indicator-border-color, transparent);
  border-radius: var(--%NS%mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --%NS%mat-focus-indicator-display: block;
    --%NS%mat-focus-indicator-fallback-border-style: none;
  }
}
`],encapsulation:2})}return t})();var xA=new b("MAT_BUTTON_CONFIG");function Yw(t){return t==null?void 0:er(t)}var Vh=(()=>{class t{_elementRef=u(V);_ngZone=u(T);_animationsDisabled=Be();_config=u(xA,{optional:!0});_focusMonitor=u(en);_cleanupClick;_renderer=u(Te);_rippleLoader=u(qw);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}showProgress=po(!1,{transform:N});constructor(){u(Me).load(nn);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",r){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,r):this._elementRef.nativeElement.focus(r)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,hostAttrs:[1,"mat-mdc-button-base"],hostVars:15,hostBindings:function(r,i){r&2&&(de("disabled",i._getDisabledAttribute())("aria-disabled",i._getAriaDisabled())("tabindex",i._getTabIndex()),Vt(i.color?"mat-"+i.color:""),U("mat-mdc-button-progress-indicator-shown",i.showProgress())("mat-mdc-button-disabled",i.disabled)("mat-mdc-button-disabled-interactive",i.disabledInteractive)("mat-unthemed",!i.color)("_mat-animation-noopable",i._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",N],disabled:[2,"disabled","disabled",N],ariaDisabled:[2,"aria-disabled","ariaDisabled",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N],tabIndex:[2,"tabIndex","tabIndex",Yw],_tabindex:[2,"tabindex","_tabindex",Yw],showProgress:[1,"showProgress"]}})}return t})(),Md=(()=>{class t extends Vh{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["*",[["","progressIndicator",""]]],r=["*","[progressIndicator]"];function i(o,a){o&1&&(Ne(0,"div",1),ce(1,1),Re())}return k({type:t,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[nt],ngContentSelectors:r,decls:5,vars:1,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(a,s){a&1&&(Oe(e),Mt(0,"span",0),ce(1),xe(2,i,2,0,"div",1),Mt(3,"span",2)(4,"span",3)),a&2&&(_(2),we(s.showProgress()?2:-1))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--%NS%mat-icon-button-state-layer-size, 40px);
  height: var(--%NS%mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--%NS%mat-icon-button-state-layer-size, 40px) - var(--%NS%mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--%NS%mat-icon-button-icon-size, 24px);
  color: var(--%NS%mat-icon-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--%NS%mat-icon-button-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface-variant) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-icon-button-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-icon-button-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-icon-button-touch-target-size, 48px);
  display: var(--%NS%mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--%NS%mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--%NS%mat-icon-button-icon-size, 24px);
  height: var(--%NS%mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__determinate-circle-graphic {
  width: inherit;
  height: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-progress-indicator-container .mdc-circular-progress__indeterminate-circle-graphic {
  height: 100%;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--%NS%mat-icon-button-container-shape, var(--%NS%mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return t})();var wA=new b("cdk-dir-doc",{providedIn:"root",factory:()=>u(G)}),CA=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function Kw(t){let n=t?.toLowerCase()||"";return n==="auto"&&typeof navigator<"u"&&navigator?.language?CA.test(navigator.language)?"rtl":"ltr":n==="rtl"?"rtl":"ltr"}var Tt=(()=>{class t{get value(){return this.valueSignal()}valueSignal=A("ltr");change=new C;constructor(){let e=u(wA,{optional:!0});if(e){let r=e.body?e.body.dir:null,i=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(Kw(r||i||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var Fe=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({})}return t})();var Fo=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Fe]})}return t})();var Zw=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),nr=(()=>{class t extends Vh{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=DA(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let r=this._elementRef.nativeElement.classList,i=this._appearance?Zw.get(this._appearance):null,o=Zw.get(e);i&&r.remove(...i),r.add(...o),this._appearance=e}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""],["",8,"material-symbols-outlined",3,"iconPositionEnd",""],["",8,"material-symbols-rounded",3,"iconPositionEnd",""],["",8,"material-symbols-sharp",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""],["","iconPositionEnd","",8,"material-symbols-outlined"],["","iconPositionEnd","",8,"material-symbols-rounded"],["","iconPositionEnd","",8,"material-symbols-sharp"]],[["","progressIndicator",""]]],r=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd]), .material-symbols-outlined:not([iconPositionEnd]), .material-symbols-rounded:not([iconPositionEnd]), .material-symbols-sharp:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd], .material-symbols-outlined[iconPositionEnd], .material-symbols-rounded[iconPositionEnd], .material-symbols-sharp[iconPositionEnd]","[progressIndicator]"];function i(o,a){o&1&&(Ne(0,"div",2),ce(1,3),Re())}return k({type:t,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[nt],ngContentSelectors:r,decls:8,vars:5,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-mdc-button-progress-indicator-container"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(a,s){a&1&&(Oe(e),Mt(0,"span",0),ce(1),Ne(2,"span",1),ce(3,1),Re(),ce(4,2),xe(5,i,2,0,"div",2),Mt(6,"span",3)(7,"span",4)),a&2&&(U("mdc-button__ripple",!s._isFab)("mdc-fab__ripple",s._isFab),_(5),we(s.showProgress()?5:-1))},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--%NS%mat-button-text-horizontal-padding, 12px);
  height: var(--%NS%mat-button-text-container-height, 40px);
  font-family: var(--%NS%mat-button-text-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-text-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-text-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-text-label-text-transform);
  font-weight: var(--%NS%mat-button-text-label-text-weight, var(--%NS%mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-text-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--%NS%mat-button-text-label-text-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, .material-symbols-outlined, .material-symbols-rounded,
.material-symbols-sharp, mat-icon, [matButtonIcon]) {
  padding: 0 var(--%NS%mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-offset, -4px);
  margin-left: var(--%NS%mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-text-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-text-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-text-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-text-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-text-touch-target-size, 48px);
  display: var(--%NS%mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-filled-container-height, 40px);
  font-family: var(--%NS%mat-button-filled-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-filled-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-filled-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-filled-label-text-transform);
  font-weight: var(--%NS%mat-button-filled-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-filled-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-filled-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-state-layer-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-filled-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-filled-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-filled-touch-target-size, 48px);
  display: var(--%NS%mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--%NS%mat-button-filled-label-text-color, var(--%NS%mat-sys-on-primary));
  background-color: var(--%NS%mat-button-filled-container-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-filled-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-mdc-unelevated-button .mat-mdc-button-progress-indicator-container {
  --%NS%mat-progress-spinner-active-indicator-color: var(--%NS%mat-button-filled-progress-active-indicator-color, var(--%NS%mat-sys-on-primary));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--%NS%mat-button-protected-container-elevation-shadow, var(--%NS%mat-sys-level1));
  height: var(--%NS%mat-button-protected-container-height, 40px);
  font-family: var(--%NS%mat-button-protected-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-protected-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-protected-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-protected-label-text-transform);
  font-weight: var(--%NS%mat-button-protected-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-protected-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-protected-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-protected-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-protected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-protected-touch-target-size, 48px);
  display: var(--%NS%mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--%NS%mat-button-protected-label-text-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-button-protected-container-color, var(--%NS%mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-protected-container-shape, var(--%NS%mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--%NS%mat-button-protected-hover-container-elevation-shadow, var(--%NS%mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--%NS%mat-button-protected-focus-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--%NS%mat-button-protected-pressed-container-elevation-shadow, var(--%NS%mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-protected-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--%NS%mat-button-protected-disabled-container-elevation-shadow, var(--%NS%mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-outlined-container-height, 40px);
  font-family: var(--%NS%mat-button-outlined-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-outlined-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-outlined-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-outlined-label-text-transform);
  font-weight: var(--%NS%mat-button-outlined-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  border-radius: var(--%NS%mat-button-outlined-container-shape, var(--%NS%mat-sys-corner-full));
  border-width: var(--%NS%mat-button-outlined-outline-width, 1px);
  padding: 0 var(--%NS%mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-outlined-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-primary) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-state-layer-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-outlined-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-outlined-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-outlined-touch-target-size, 48px);
  display: var(--%NS%mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--%NS%mat-button-outlined-label-text-color, var(--%NS%mat-sys-primary));
  border-color: var(--%NS%mat-button-outlined-outline-color, var(--%NS%mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: var(--%NS%mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--%NS%mat-button-tonal-container-height, 40px);
  font-family: var(--%NS%mat-button-tonal-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-tonal-label-text-size, var(--%NS%mat-sys-label-large-size));
  letter-spacing: var(--%NS%mat-button-tonal-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
  text-transform: var(--%NS%mat-button-tonal-label-text-transform);
  font-weight: var(--%NS%mat-button-tonal-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  padding: 0 var(--%NS%mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--%NS%mat-button-tonal-label-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-tonal-container-color, var(--%NS%mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--%NS%mat-button-tonal-container-shape, var(--%NS%mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--%NS%mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-offset, -8px);
  margin-left: var(--%NS%mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--%NS%mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--%NS%mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--%NS%mat-button-tonal-ripple-color, color-mix(in srgb, var(--%NS%mat-sys-on-secondary-container) calc(var(--%NS%mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-state-layer-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--%NS%mat-button-tonal-disabled-state-layer-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--%NS%mat-button-tonal-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--%NS%mat-button-tonal-touch-target-size, 48px);
  display: var(--%NS%mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-mdc-button-progress-indicator-container {
  position: absolute;
  inset-inline-start: 0;
  inset-block-start: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 100%;
  box-sizing: border-box;
}

.mat-mdc-button-progress-indicator-shown mat-icon,
.mat-mdc-button-progress-indicator-shown [matButtonIcon],
.mat-mdc-button-progress-indicator-shown .mdc-button__label {
  visibility: hidden;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2})})()}return t})();function DA(t){return t.hasAttribute("mat-raised-button")?"elevated":t.hasAttribute("mat-stroked-button")?"outlined":t.hasAttribute("mat-flat-button")?"filled":t.hasAttribute("mat-button")?"text":null}var Bn=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Fo,Fe]})}return t})();var rC=(()=>{class t{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,r){this._renderer=e,this._elementRef=r}setProperty(e,r){this._renderer.setProperty(this._elementRef.nativeElement,e,r)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(r){return new(r||t)(Ze(Te),Ze(V))};static \u0275dir=j({type:t})}return t})(),SA=(()=>{class t extends rC{static \u0275fac=(()=>{let e;return function(i){return(e||(e=gn(t)))(i||t)}})();static \u0275dir=j({type:t,features:[nt]})}return t})(),wn=new b("");var EA={provide:wn,useExisting:$e(()=>iC),multi:!0};function kA(){let t=Xt()?Xt().getUserAgent():"";return/android (\d+)/.test(t.toLowerCase())}var IA=new b(""),iC=(()=>{class t extends rC{_compositionMode;_composing=!1;constructor(e,r,i){super(e,r),this._compositionMode=i,this._compositionMode==null&&(this._compositionMode=!kA())}writeValue(e){let r=e??"";this.setProperty("value",r)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(r){return new(r||t)(Ze(Te),Ze(V),Ze(IA,8))};static \u0275dir=j({type:t,selectors:[["input","formControlName","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControlName","",3,"ngNoCva",""],["input","formControl","",3,"type","checkbox",3,"ngNoCva",""],["textarea","formControl","",3,"ngNoCva",""],["input","ngModel","",3,"type","checkbox",3,"ngNoCva",""],["textarea","ngModel","",3,"ngNoCva",""],["","ngDefaultControl",""]],hostBindings:function(r,i){r&1&&B("input",function(a){return i._handleInput(a.target.value)})("blur",function(){return i.onTouched()})("compositionstart",function(){return i._compositionStart()})("compositionend",function(a){return i._compositionEnd(a.target.value)})},standalone:!1,features:[ke([EA]),nt]})}return t})();function Hh(t){return t==null||Uh(t)===0}function Uh(t){return t==null?null:Array.isArray(t)||typeof t=="string"?t.length:t instanceof Set?t.size:null}var rr=new b(""),oC=new b(""),MA=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,ki=class{static min(n){return NA(n)}static max(n){return TA(n)}static required(n){return aC(n)}static requiredTrue(n){return AA(n)}static email(n){return RA(n)}static minLength(n){return OA(n)}static maxLength(n){return FA(n)}static pattern(n){return PA(n)}static nullValidator(n){return Td()}static compose(n){return mC(n)}static composeAsync(n){return fC(n)}};function NA(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e<t?{min:{min:t,actual:n.value}}:null}}function TA(t){return n=>{if(n.value==null||t==null)return null;let e=parseFloat(n.value);return!isNaN(e)&&e>t?{max:{max:t,actual:n.value}}:null}}function aC(t){return Hh(t.value)?{required:!0}:null}function AA(t){return t.value===!0?null:{required:!0}}function RA(t){return Hh(t.value)||MA.test(t.value)?null:{email:!0}}function OA(t){return n=>{let e=n.value?.length??Uh(n.value);return e===null||e===0?null:e<t?{minlength:{requiredLength:t,actualLength:e}}:null}}function FA(t){return n=>{let e=n.value?.length??Uh(n.value);return e!==null&&e>t?{maxlength:{requiredLength:t,actualLength:e}}:null}}function PA(t){if(!t)return Td;let n,e;return typeof t=="string"?(e="",t.charAt(0)!=="^"&&(e+="^"),e+=t,t.charAt(t.length-1)!=="$"&&(e+="$"),n=new RegExp(e)):(e=t.toString(),n=t),r=>{if(Hh(r.value))return null;let i=r.value;return n.test(i)?null:{pattern:{requiredPattern:e,actualValue:i}}}}function Td(t){return null}function sC(t){return t!=null}function lC(t){return Xn(t)?Pe(t):t}function cC(t){let n={};return t.forEach(e=>{n=e!=null?g(g({},n),e):n}),Object.keys(n).length===0?null:n}function dC(t,n){return n.map(e=>e(t))}function LA(t){return!t.validate}function uC(t){return t.map(n=>LA(n)?n:e=>n.validate(e))}function mC(t){if(!t)return null;let n=t.filter(sC);return n.length==0?null:function(e){return cC(dC(e,n))}}function $h(t){return t!=null?mC(uC(t)):null}function fC(t){if(!t)return null;let n=t.filter(sC);return n.length==0?null:function(e){let r=dC(e,n).map(lC);return zu(r).pipe(_e(cC))}}function Gh(t){return t!=null?fC(uC(t)):null}function Qw(t,n){return t===null?[n]:Array.isArray(t)?[...t,n]:[t,n]}function pC(t){return t._rawValidators}function hC(t){return t._rawAsyncValidators}function Bh(t){return t?Array.isArray(t)?t:[t]:[]}function Ad(t,n){return Array.isArray(t)?t.includes(n):t===n}function Xw(t,n){let e=Bh(n);return Bh(t).forEach(i=>{Ad(e,i)||e.push(i)}),e}function Jw(t,n){return Bh(n).filter(e=>!Ad(t,e))}var Rd=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(n){this._rawValidators=n||[],this._composedValidatorFn=$h(this._rawValidators)}_setAsyncValidators(n){this._rawAsyncValidators=n||[],this._composedAsyncValidatorFn=Gh(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(n){this._onDestroyCallbacks.push(n)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(n=>n()),this._onDestroyCallbacks=[]}reset(n=void 0){this.control?.reset(n)}hasError(n,e){return this.control?this.control.hasError(n,e):!1}getError(n,e){return this.control?this.control.getError(n,e):null}},Ii=class extends Rd{name;get formDirective(){return null}get path(){return null}};var ks="VALID",Nd="INVALID",Po="PENDING",Is="DISABLED",Nr=class{},Od=class extends Nr{value;source;constructor(n,e){super(),this.value=n,this.source=e}},Ns=class extends Nr{pristine;source;constructor(n,e){super(),this.pristine=n,this.source=e}},Ts=class extends Nr{touched;source;constructor(n,e){super(),this.touched=n,this.source=e}},Lo=class extends Nr{status;source;constructor(n,e){super(),this.status=n,this.source=e}},Fd=class extends Nr{source;constructor(n){super(),this.source=n}},Vo=class extends Nr{source;constructor(n){super(),this.source=n}};function gC(t){return(Bd(t)?t.validators:t)||null}function VA(t){return Array.isArray(t)?$h(t):t||null}function bC(t,n){return(Bd(n)?n.asyncValidators:t)||null}function jA(t){return Array.isArray(t)?Gh(t):t||null}function Bd(t){return t!=null&&!Array.isArray(t)&&typeof t=="object"}function BA(t,n,e){let r=t.controls;if(!(n?Object.keys(r):r).length)throw new D(1e3,"");if(!vC(r,e))throw new D(1001,"")}function zA(t,n,e){t._forEachChild((r,i)=>{if(e[i]===void 0)throw new D(-1002,"")})}var Pd=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_hasRequired=A(!1);_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(n,e){this._assignValidators(n),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(n){this._rawValidators=this._composedValidatorFn=n,this._updateHasRequiredValidator()}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(n){this._rawAsyncValidators=this._composedAsyncValidatorFn=n}get parent(){return this._parent}get status(){return Ie(this.statusReactive)}set status(n){Ie(()=>this.statusReactive.set(n))}_status=ft(()=>this.statusReactive());statusReactive=A(void 0);get valid(){return this.status===ks}get invalid(){return this.status===Nd}get pending(){return this.status===Po}get disabled(){return this.status===Is}get enabled(){return this.status!==Is}errors;get pristine(){return Ie(this.pristineReactive)}set pristine(n){Ie(()=>this.pristineReactive.set(n))}_pristine=ft(()=>this.pristineReactive());pristineReactive=A(!0);get dirty(){return!this.pristine}get touched(){return Ie(this.touchedReactive)}set touched(n){Ie(()=>this.touchedReactive.set(n))}_touched=ft(()=>this.touchedReactive());touchedReactive=A(!1);get untouched(){return!this.touched}_events=new E;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(n){this._assignValidators(n)}setAsyncValidators(n){this._assignAsyncValidators(n)}addValidators(n){this.setValidators(Xw(n,this._rawValidators))}addAsyncValidators(n){this.setAsyncValidators(Xw(n,this._rawAsyncValidators))}removeValidators(n){this.setValidators(Jw(n,this._rawValidators))}removeAsyncValidators(n){this.setAsyncValidators(Jw(n,this._rawAsyncValidators))}hasValidator(n){return Ad(this._rawValidators,n)}hasAsyncValidator(n){return Ad(this._rawAsyncValidators,n)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(n={}){let e=this.touched===!1;this.touched=!0;let r=n.sourceControl??this;n.onlySelf||this._parent?.markAsTouched(R(g({},n),{sourceControl:r})),e&&n.emitEvent!==!1&&this._events.next(new Ts(!0,r))}markAllAsDirty(n={}){this.markAsDirty({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(n))}markAllAsTouched(n={}){this.markAsTouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(n))}markAsUntouched(n={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let r=n.sourceControl??this;this._forEachChild(i=>{i.markAsUntouched({onlySelf:!0,emitEvent:n.emitEvent,sourceControl:r})}),n.onlySelf||this._parent?._updateTouched(n,r),e&&n.emitEvent!==!1&&this._events.next(new Ts(!1,r))}markAsDirty(n={}){let e=this.pristine===!0;this.pristine=!1;let r=n.sourceControl??this;n.onlySelf||this._parent?.markAsDirty(R(g({},n),{sourceControl:r})),e&&n.emitEvent!==!1&&this._events.next(new Ns(!1,r))}markAsPristine(n={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let r=n.sourceControl??this;this._forEachChild(i=>{i.markAsPristine({onlySelf:!0,emitEvent:n.emitEvent})}),n.onlySelf||this._parent?._updatePristine(n,r),e&&n.emitEvent!==!1&&this._events.next(new Ns(!0,r))}markAsPending(n={}){this.status=Po;let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Lo(this.status,e)),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.markAsPending(R(g({},n),{sourceControl:e}))}disable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=Is,this.errors=null,this._forEachChild(i=>{i.disable(R(g({},n),{onlySelf:!0}))}),this._updateValue();let r=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Od(this.value,r)),this._events.next(new Lo(this.status,r)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(R(g({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!0))}enable(n={}){let e=this._parentMarkedDirty(n.onlySelf);this.status=ks,this._forEachChild(r=>{r.enable(R(g({},n),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent}),this._updateAncestors(R(g({},n),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!1))}_updateAncestors(n,e){n.onlySelf||(this._parent?.updateValueAndValidity(n),n.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(n){this._parent=n}getRawValue(){return this.value}updateValueAndValidity(n={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let r=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===ks||this.status===Po)&&this._runAsyncValidator(r,n.emitEvent)}let e=n.sourceControl??this;n.emitEvent!==!1&&(this._events.next(new Od(this.value,e)),this._events.next(new Lo(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),n.onlySelf||this._parent?.updateValueAndValidity(R(g({},n),{sourceControl:e}))}_updateTreeValidity(n={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(n)),this.updateValueAndValidity({onlySelf:!0,emitEvent:n.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Is:ks}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(n,e){if(this.asyncValidator){this.status=Po,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:n!==!1};let r=lC(this.asyncValidator(this));this._asyncValidationSubscription=r.subscribe(i=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(i,{emitEvent:e,shouldHaveEmitted:n})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let n=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,n}return!1}setErrors(n,e={}){this.errors=n,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(n){let e=n;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((r,i)=>r&&r._find(i),this)}getError(n,e){let r=e?this.get(e):this;return r?.errors?r.errors[n]:null}hasError(n,e){return!!this.getError(n,e)}get root(){let n=this;for(;n._parent;)n=n._parent;return n}_updateControlsErrors(n,e,r){this.status=this._calculateStatus(),n&&this.statusChanges.emit(this.status),(n||r)&&this._events.next(new Lo(this.status,e)),this._parent&&this._parent._updateControlsErrors(n,e,r)}_initObservables(){this.valueChanges=new C,this.statusChanges=new C}_calculateStatus(){return this._allControlsDisabled()?Is:this.errors?Nd:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Po)?Po:this._anyControlsHaveStatus(Nd)?Nd:ks}_anyControlsHaveStatus(n){return this._anyControls(e=>e.status===n)}_anyControlsDirty(){return this._anyControls(n=>n.dirty)}_anyControlsTouched(){return this._anyControls(n=>n.touched)}_updatePristine(n,e){let r=!this._anyControlsDirty(),i=this.pristine!==r;this.pristine=r,n.onlySelf||this._parent?._updatePristine(n,e),i&&this._events.next(new Ns(this.pristine,e))}_updateTouched(n={},e){this.touched=this._anyControlsTouched(),this._events.next(new Ts(this.touched,e)),n.onlySelf||this._parent?._updateTouched(n,e)}_onDisabledChange=[];_registerOnCollectionChange(n){this._onCollectionChange=n}_setUpdateStrategy(n){Bd(n)&&n.updateOn!=null&&(this._updateOn=n.updateOn)}_parentMarkedDirty(n){return!n&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(n){return null}_assignValidators(n){this._rawValidators=Array.isArray(n)?n.slice():n,this._composedValidatorFn=VA(this._rawValidators),this._updateHasRequiredValidator()}_assignAsyncValidators(n){this._rawAsyncValidators=Array.isArray(n)?n.slice():n,this._composedAsyncValidatorFn=jA(this._rawAsyncValidators)}_updateHasRequiredValidator(){Ie(()=>this._hasRequired.set(this.hasValidator(ki.required)))}};function vC(t,n){return Object.hasOwn(t,n)}function HA(t){return t.tagName==="INPUT"||t.tagName==="SELECT"||t.tagName==="TEXTAREA"}function UA(t,n,e,r){switch(e){case"name":t.setAttribute(n,e,r);break;case"disabled":case"readonly":case"required":r?t.setAttribute(n,e,""):t.removeAttribute(n,e);break;case"max":case"min":case"minLength":case"maxLength":r!==void 0?t.setAttribute(n,e,r.toString()):t.removeAttribute(n,e);break}}var zh=class{kind;context;control;message;constructor({kind:n,context:e,control:r}){this.kind=n,this.context=e,this.control=r}};var $A=(()=>{class t{_validator=Td;_onChange;_enabled;ngOnChanges(e){if(this.inputName in e){let r=this.normalizeInput(e[this.inputName].currentValue);this._enabled=this.enabled(r),this._validator=this._enabled?this.createValidator(r):Td,this._onChange?.()}}validate(e){return this._validator(e)}registerOnValidatorChange(e){this._onChange=e}enabled(e){return e!=null}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,features:[Ae]})}return t})();var GA={provide:rr,useExisting:$e(()=>yC),multi:!0};var yC=(()=>{class t extends $A{required;inputName="required";normalizeInput=N;createValidator=e=>aC;enabled(e){return e}static \u0275fac=(()=>{let e;return function(i){return(e||(e=gn(t)))(i||t)}})();static \u0275dir=j({type:t,selectors:[["","required","","formControlName","",3,"type","checkbox"],["","required","","formControl","",3,"type","checkbox"],["","required","","ngModel","",3,"type","checkbox"]],hostVars:1,hostBindings:function(r,i){r&2&&de("required",i._enabled?"":null)},inputs:{required:"required"},standalone:!1,features:[ke([GA]),nt]})}return t})();var _C=new b("",{factory:()=>WA}),WA="always";function eC(t,n,e=!0){let r=()=>{};n?.valueAccessor?.registerOnChange(r),n?.valueAccessor?.registerOnTouched(r),Vd(t,n),t&&(n._invokeOnDestroyCallbacks(),t._registerOnCollectionChange(()=>{}))}function Ld(t,n){t.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(n)})}function xC(t,n){let e=pC(t);n.validator!==null?t.setValidators(Qw(e,n.validator)):typeof e=="function"&&t.setValidators([e]);let r=hC(t);n.asyncValidator!==null?t.setAsyncValidators(Qw(r,n.asyncValidator)):typeof r=="function"&&t.setAsyncValidators([r]);let i=()=>t.updateValueAndValidity();Ld(n._rawValidators,i),Ld(n._rawAsyncValidators,i)}function Vd(t,n){let e=!1;if(t!==null){if(n.validator!==null){let i=pC(t);if(Array.isArray(i)&&i.length>0){let o=i.filter(a=>a!==n.validator);o.length!==i.length&&(e=!0,t.setValidators(o))}}if(n.asyncValidator!==null){let i=hC(t);if(Array.isArray(i)&&i.length>0){let o=i.filter(a=>a!==n.asyncValidator);o.length!==i.length&&(e=!0,t.setAsyncValidators(o))}}}let r=()=>{};return Ld(n._rawValidators,r),Ld(n._rawAsyncValidators,r),e}function wC(t,n){t==null,xC(t,n)}function qA(t,n){return Vd(t,n)}function YA(t){return Object.getPrototypeOf(t.constructor)===SA}function CC(t,n){t._syncPendingControls(),n.forEach(e=>{let r=e.control;r.updateOn==="submit"&&r._pendingChange&&(e.viewToModelUpdate(r._pendingValue),r._pendingChange=!1)})}function KA(t,n){if(!n)return null;Array.isArray(n);let e,r,i;return n.forEach(o=>{o.constructor===iC?e=o:YA(o)?r=o:i=o}),i||r||e||null}function ZA(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}var As=class extends Rd{_parent=null;name=null;valueAccessor=null;isCustomControlBased=!1;userOnReset;resetSubscription;set onReset(n){this.userOnReset=n,this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.control&&(this.resetSubscription=this.control.events.subscribe(e=>{e instanceof Vo&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription?.add(this.resetSubscription))}isNativeFormElement=!1;rawValueAccessors;_selectedValueAccessor=null;get selectedValueAccessor(){return this._selectedValueAccessor??=KA(this,this.rawValueAccessors)}parseErrorsValidator=null;renderer;injector;requiredValidatorViaDi;subscription;customControlBindings=null;constructor(n,e,r){super(),this.injector=n,this.renderer=e,this.rawValueAccessors=r,this.injector?.get(at)?.onDestroy(()=>{this.removeParseErrorsValidator(this.control),this.subscription?.unsubscribe()})}setupCustomControl(){this.subscription?.unsubscribe();let n=this.injector?.get(ve);if(!this.control||!n)return;let e=n.markForCheck.bind(n);this.subscription=new ue,this.subscription.add(this.control.valueChanges.subscribe(e)),this.subscription.add(this.control.statusChanges.subscribe(e)),this.resetSubscription?.unsubscribe(),this.resetSubscription=void 0,this.userOnReset&&(this.resetSubscription=this.control.events.subscribe(r=>{r instanceof Vo&&this.control&&this.userOnReset?.(this.control.value)}),this.subscription.add(this.resetSubscription)),this.parseErrorsValidator&&this.control.addValidators(this.parseErrorsValidator)}ngControlCreate(n){!n.nativeElement.hasAttribute?.("ngNoCva")&&(this.rawValueAccessors&&this.rawValueAccessors.length>0||this.valueAccessor!==null)||!n.customControl||(this.isCustomControlBased=!0,n.listenToCustomControlModel(i=>{this.control?.markAsDirty(),this.control?.setValue(i,{emitModelToViewChange:!1}),this.viewToModelUpdate(i)}),n.listenToCustomControlOutput("touch",()=>{this.control?.markAsTouched()}),this.customControlBindings={},this.isNativeFormElement=HA(n.nativeElement),this.requiredValidatorViaDi=this._rawValidators.find(i=>i instanceof yC))}ngControlUpdate(n,e){if(!this.isCustomControlBased)return;let r=this.control,i=this.customControlBindings;Object.is(i.value,r.value)||(i.value=r.value,n.setCustomControlModelInput(r.value)),this.bindControlProperty(n,i,"touched",r.touched),this.bindControlProperty(n,i,"dirty",r.dirty),this.bindControlProperty(n,i,"valid",r.valid),this.bindControlProperty(n,i,"invalid",r.invalid),this.bindControlProperty(n,i,"pending",r.pending),this.bindControlProperty(n,i,"disabled",r.disabled),this.shouldBindRequired&&this.bindControlProperty(n,i,"required",this.isRequired);let o=r.errors;if(i.errors!==o){i.errors=o;let a=this._convertErrors(o);n.setInputOnDirectives("errors",a)}}get isRequired(){return(this.requiredValidatorViaDi?._enabled||this.control?._hasRequired())??!1}get shouldBindRequired(){return!0}bindControlProperty(n,e,r,i){if(e[r]===i)return;e[r]=i;let o=n.setInputOnDirectives(r,i);this.isNativeFormElement&&!o&&(r==="disabled"||r==="required")&&this.renderer&&UA(this.renderer,n.nativeElement,r,i)}_convertErrors(n){if(n===null)return[];let e=this.control;return Object.entries(n).map(([r,i])=>new zh({context:i,kind:r,control:e}))}setParseErrorSource(n){if(n===void 0)return;let e=null,r=ft(()=>{let i=n();return i.length===0?null:i.reduce((o,a)=>(o[a.kind]=a,o),{})});this.parseErrorsValidator=(()=>e).bind(this),Nn(()=>{e=r(),this.control?.updateValueAndValidity({emitEvent:!1})},{injector:this.injector})}removeParseErrorsValidator(n){this.parseErrorsValidator&&(n?.removeValidators(this.parseErrorsValidator),n?.updateValueAndValidity({emitEvent:!1}))}};var jd=class extends Pd{constructor(n,e,r){super(gC(e),bC(r,e)),this.controls=n,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(n,e){let r=this._find(n);return r||(this.controls[n]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(n,e,r={}){this.registerControl(n,e),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}removeControl(n,e={}){let r=this._find(n);r&&r._registerOnCollectionChange(()=>{}),delete this.controls[n],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(n,e,r={}){let i=this._find(n);i&&i._registerOnCollectionChange(()=>{}),delete this.controls[n],e&&this.registerControl(n,e),this.updateValueAndValidity({emitEvent:r.emitEvent}),this._onCollectionChange()}contains(n){return this._find(n)?.enabled===!0}setValue(n,e={}){Ie(()=>{zA(this,!0,n),Object.keys(n).forEach(r=>{BA(this,!0,r),this.controls[r].setValue(n[r],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)})}patchValue(n,e={}){n!=null&&(Object.keys(n).forEach(r=>{let i=this._find(r);i&&i.patchValue(n[r],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(n={},e={}){this._forEachChild((r,i)=>{r.reset(n?n[i]:null,R(g({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Vo(this))}getRawValue(){return this._reduceChildren({},(n,e,r)=>(n[r]=e.getRawValue(),n))}_syncPendingControls(){let n=this._reduceChildren(!1,(e,r)=>r._syncPendingControls()?!0:e);return n&&this.updateValueAndValidity({onlySelf:!0}),n}_forEachChild(n){Object.keys(this.controls).forEach(e=>{let r=this.controls[e];r&&n(r,e)})}_setUpControls(){this._forEachChild(n=>{n.setParent(this),n._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(n){for(let[e,r]of Object.entries(this.controls))if(this.contains(e)&&n(r))return!0;return!1}_reduceValue(){let n={};return this._reduceChildren(n,(e,r,i)=>((r.enabled||this.disabled)&&(e[i]=r.value),e))}_reduceChildren(n,e){let r=n;return this._forEachChild((i,o)=>{r=e(r,i,o)}),r}_allControlsDisabled(){for(let n of Object.keys(this.controls))if(this.controls[n].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(n){return vC(this.controls,n)?this.controls[n]:null}};var QA={provide:Ii,useExisting:$e(()=>zd)},Ms=Promise.resolve(),zd=(()=>{class t extends Ii{callSetDisabledState;get submitted(){return Ie(this.submittedReactive)}_submitted=ft(()=>this.submittedReactive());submittedReactive=A(!1);_directives=new Set;form;ngSubmit=new C;options;constructor(e,r,i){super(),this.callSetDisabledState=i,this.form=new jd({},$h(e),Gh(r))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Ms.then(()=>{let r=this._findContainer(e.path);e.control=r.registerControl(e.name,e.control),e._setupWithForm(this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Ms.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Ms.then(()=>{let r=this._findContainer(e.path),i=new jd({});wC(i,e),r.registerControl(e.name,i),i.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Ms.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,r){Ms.then(()=>{this.form.get(e.path).setValue(r)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),CC(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Fd(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(r){return new(r||t)(Ze(rr,10),Ze(oC,10),Ze(_C,8))};static \u0275dir=j({type:t,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(r,i){r&1&&B("submit",function(a){return i.onSubmit(a)})("reset",function(){return i.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ke([QA]),nt]})}return t})();function tC(t,n){let e=t.indexOf(n);e>-1&&t.splice(e,1)}function nC(t){return typeof t=="object"&&t!==null&&Object.keys(t).length===2&&"value"in t&&"disabled"in t}var XA=class extends Pd{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(n=null,e,r){super(gC(e),bC(r,e)),this._applyFormState(n),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Bd(e)&&(e.nonNullable||e.initialValueIsDefault)&&(nC(n)?this.defaultValue=n.value:this.defaultValue=n)}setValue(n,e={}){Ie(()=>{this.value=this._pendingValue=n,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(r=>r(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)})}patchValue(n,e={}){this.setValue(n,e)}reset(n=this.defaultValue,e={}){this._applyFormState(n),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Vo(this))}_updateValue(){}_anyControls(n){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(n){this._onChange.push(n)}_unregisterOnChange(n){tC(this._onChange,n)}registerOnDisabledChange(n){this._onDisabledChange.push(n)}_unregisterOnDisabledChange(n){tC(this._onDisabledChange,n)}_forEachChild(n){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(n){nC(n)?(this.value=this._pendingValue=n.value,n.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=n}};var JA=t=>t instanceof XA;var eR=(()=>{class t extends Ii{callSetDisabledState;get submitted(){return Ie(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=ft(()=>this._submittedReactive());_submittedReactive=A(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,r,i){super(),this.callSetDisabledState=i,this._setValidators(e),this._setAsyncValidators(r)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){if(!this.form)throw new D(1052,!1);Object.hasOwn(e,"form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Vd(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let r=this.form.get(e.path);return e._setupWithForm(r,this.callSetDisabledState),r.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),r}getControl(e){return this.form.get(e.path)}removeControl(e){eC(e.control||null,e,!1),ZA(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,r){this.form.get(e.path).setValue(r)}onReset(){this.resetForm()}resetForm(e=void 0,r={}){this.form.reset(e,r),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,CC(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Fd(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let r=e.control,i=this.form.get(e.path);r!==i&&(eC(r||null,e),JA(i)&&e._setupWithForm(i,this.callSetDisabledState))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let r=this.form.get(e.path);wC(r,e),r.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let r=this.form?.get(e.path);r&&qA(r,e)&&r.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){xC(this.form,this),this._oldForm&&Vd(this._oldForm,this)}static \u0275fac=function(r){return new(r||t)(Ze(rr,10),Ze(oC,10),Ze(_C,8))};static \u0275dir=j({type:t,features:[nt,Ae]})}return t})(),tR={provide:Ii,useExisting:$e(()=>Hd)},Hd=(()=>{class t extends eR{form=null;ngSubmit=new C;get control(){return this.form}static \u0275fac=(()=>{let e;return function(i){return(e||(e=gn(t)))(i||t)}})();static \u0275dir=j({type:t,selectors:[["","formGroup",""]],hostBindings:function(r,i){r&1&&B("submit",function(a){return i.onSubmit(a)})("reset",function(){return i.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ke([tR]),nt]})}return t})();var jo=(()=>{class t{labelPosition="after";static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){return k({type:t,selectors:[["","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,o){i&2&&U("mdc-form-field--align-end",o.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},ngContentSelectors:["*"],decls:1,vars:0,template:function(i,o){i&1&&(Oe(),ce(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label, .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label, [dir=rtl] .mat-internal-form-field > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label, .mdc-form-field--align-end > .mat-internal-form-field-label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label, [dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end .mat-internal-form-field-label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2})})()}return t})();var Wh={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},nR=new b("mat-checkbox-default-options",{providedIn:"root",factory:()=>Wh}),yt=(function(t){return t[t.Init=0]="Init",t[t.Checked=1]="Checked",t[t.Unchecked=2]="Unchecked",t[t.Indeterminate=3]="Indeterminate",t})(yt||{}),qh=class{source;checked},Yh=(()=>{class t{_elementRef=u(V);_changeDetectorRef=u(ve);_ngZone=u(T);_animationsDisabled=Be();_options=u(nR,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let r=new qh;return r.source=this,r.checked=e,r}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new C;indeterminateChange=new C;value;disableRipple=!1;_inputElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=yt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){u(Me).load(nn);let e=u(new jt("tabindex"),{optional:!0});this._options=this._options||Wh,this.color=this._options.color||Wh.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=u(je).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let r=e!=this._indeterminate();this._indeterminate.set(e),r&&(e?this._transitionCheckState(yt.Indeterminate):this._transitionCheckState(this.checked?yt.Checked:yt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=A(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let r=this._currentCheckState,i=this._getAnimationTargetElement();if(!(r===e||!i)&&(this._currentAnimationClass&&i.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(r,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){i.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{i.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?yt.Checked:yt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,r){if(this._animationsDisabled)return"";switch(e){case yt.Init:if(r===yt.Checked)return this._animationClasses.uncheckedToChecked;if(r==yt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case yt.Unchecked:return r===yt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case yt.Checked:return r===yt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case yt.Indeterminate:return r===yt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let r=this._inputElement;r&&(r.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_preventBubblingFromLabel(e){e.target&&this._inputElement&&e.target!==this._inputElement.nativeElement&&e.stopPropagation()}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["input"],r=["*"];return k({type:t,selectors:[["mat-checkbox"]],viewQuery:function(o,a){if(o&1&&He(e,5),o&2){let s;ae(s=se())&&(a._inputElement=s.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(o,a){o&2&&(mt("id",a.id),de("tabindex",null)("aria-label",null)("aria-labelledby",null),Vt(a.color?"mat-"+a.color:"mat-accent"),U("_mat-animation-noopable",a._animationsDisabled)("mdc-checkbox--disabled",a.disabled)("mat-mdc-checkbox-disabled",a.disabled)("mat-mdc-checkbox-checked",a.checked)("mat-mdc-checkbox-disabled-interactive",a.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",N],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",N],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",N],tabIndex:[2,"tabIndex","tabIndex",i=>i==null?void 0:er(i)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",N],checked:[2,"checked","checked",N],disabled:[2,"disabled","disabled",N],indeterminate:[2,"indeterminate","indeterminate",N]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[ke([{provide:wn,useExisting:$e(()=>t),multi:!0},{provide:rr,useExisting:t,multi:!0}]),Ae],ngContentSelectors:r,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition","for"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(o,a){if(o&1&&(Oe(),v(0,"label",3),B("click",function(l){return a._preventBubblingFromLabel(l)}),v(1,"span",4,0),Z(3,"span",5),v(4,"input",6,1),B("blur",function(){return a._onBlur()})("click",function(){return a._onInputClick()})("change",function(l){return a._onInteractionEvent(l)}),y(),Z(6,"span",7),v(7,"span",8),un(),v(8,"svg",9),Z(9,"path",10),y(),si(),Z(10,"span",11),y(),Z(11,"span",12),y(),v(12,"span",13,2),ce(14),y()()),o&2){let s=Zt(2);z("labelPosition",a.labelPosition)("for",a.inputId),_(4),U("mdc-checkbox--selected",a.checked),z("checked",a.checked)("indeterminate",a.indeterminate)("disabled",a.disabled&&!a.disabledInteractive)("id",a.inputId)("required",a.required)("tabIndex",a.disabled&&!a.disabledInteractive?-1:a.tabIndex),de("aria-label",a.ariaLabel||null)("aria-labelledby",a.ariaLabelledby)("aria-describedby",a.ariaDescribedby)("aria-checked",a.indeterminate?"mixed":null)("aria-controls",a.ariaControls)("aria-disabled",a.disabled&&a.disabledInteractive?!0:null)("aria-expanded",a.ariaExpanded)("aria-owns",a.ariaOwns)("name",a.name)("value",a.value),_(7),z("matRippleTrigger",s)("matRippleDisabled",a.disableRipple||a.disabled)("matRippleCentered",!0)}},dependencies:[Mr,jo],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-unselected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--%NS%mat-checkbox-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--%NS%mat-checkbox-selected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--%NS%mat-checkbox-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-checkbox-state-layer-size, 40px);
  height: var(--%NS%mat-checkbox-state-layer-size, 40px);
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - var(--%NS%mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--%NS%mat-checkbox-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  top: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--%NS%mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
  background-color: var(--%NS%mat-checkbox-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--%NS%mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--%NS%mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--%NS%mat-checkbox-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--%NS%mat-checkbox-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--%NS%mat-checkbox-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-checkbox-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-checkbox-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-checkbox-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-checkbox-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-checkbox-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
  color: var(--%NS%mat-checkbox-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled .mat-internal-form-field-label {
    color: GrayText;
  }
}
.mat-mdc-checkbox .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-checkbox-touch-target-size, 48px);
  width: var(--%NS%mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})})()}return t})(),Ud=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Yh,Fe]})}return t})();var iR=new b("MAT_CARD_CONFIG"),At=(()=>{class t{appearance;constructor(){let e=u(iR,{optional:!0});this.appearance=e?.appearance||"raised"}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){return k({type:t,selectors:[["mat-card"]],hostAttrs:[1,"mat-mdc-card","mdc-card"],hostVars:8,hostBindings:function(i,o){i&2&&U("mat-mdc-card-outlined",o.appearance==="outlined")("mdc-card--outlined",o.appearance==="outlined")("mat-mdc-card-filled",o.appearance==="filled")("mdc-card--filled",o.appearance==="filled")},inputs:{appearance:"appearance"},exportAs:["matCard"],ngContentSelectors:["*"],decls:1,vars:0,template:function(i,o){i&1&&(Oe(),ce(0))},styles:[`.mat-mdc-card {
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
  position: relative;
  border-style: solid;
  border-width: 0;
  background-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-color: var(--%NS%mat-card-elevated-container-color, var(--%NS%mat-sys-surface-container-low));
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-elevated-container-elevation, var(--%NS%mat-sys-level1));
}
.mat-mdc-card::after {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  border: solid 1px transparent;
  content: "";
  display: block;
  pointer-events: none;
  box-sizing: border-box;
  border-radius: var(--%NS%mat-card-elevated-container-shape, var(--%NS%mat-sys-corner-medium));
}

.mat-mdc-card-outlined {
  background-color: var(--%NS%mat-card-outlined-container-color, var(--%NS%mat-sys-surface));
  border-radius: var(--%NS%mat-card-outlined-container-shape, var(--%NS%mat-sys-corner-medium));
  border-width: var(--%NS%mat-card-outlined-outline-width, 1px);
  border-color: var(--%NS%mat-card-outlined-outline-color, var(--%NS%mat-sys-outline-variant));
  box-shadow: var(--%NS%mat-card-outlined-container-elevation, var(--%NS%mat-sys-level0));
}
.mat-mdc-card-outlined::after {
  border: none;
}

.mat-mdc-card-filled {
  background-color: var(--%NS%mat-card-filled-container-color, var(--%NS%mat-sys-surface-container-highest));
  border-radius: var(--%NS%mat-card-filled-container-shape, var(--%NS%mat-sys-corner-medium));
  box-shadow: var(--%NS%mat-card-filled-container-elevation, var(--%NS%mat-sys-level0));
}

.mdc-card__media {
  position: relative;
  box-sizing: border-box;
  background-repeat: no-repeat;
  background-position: center;
  background-size: cover;
}
.mdc-card__media::before {
  display: block;
  content: "";
}
.mdc-card__media:first-child {
  border-top-left-radius: inherit;
  border-top-right-radius: inherit;
}
.mdc-card__media:last-child {
  border-bottom-left-radius: inherit;
  border-bottom-right-radius: inherit;
}

.mat-mdc-card-actions {
  display: flex;
  flex-direction: row;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  padding: 8px;
}

.mat-mdc-card-title {
  font-family: var(--%NS%mat-card-title-text-font, var(--%NS%mat-sys-title-large-font));
  line-height: var(--%NS%mat-card-title-text-line-height, var(--%NS%mat-sys-title-large-line-height));
  font-size: var(--%NS%mat-card-title-text-size, var(--%NS%mat-sys-title-large-size));
  letter-spacing: var(--%NS%mat-card-title-text-tracking, var(--%NS%mat-sys-title-large-tracking));
  font-weight: var(--%NS%mat-card-title-text-weight, var(--%NS%mat-sys-title-large-weight));
}

.mat-mdc-card-subtitle {
  color: var(--%NS%mat-card-subtitle-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-card-subtitle-text-font, var(--%NS%mat-sys-title-medium-font));
  line-height: var(--%NS%mat-card-subtitle-text-line-height, var(--%NS%mat-sys-title-medium-line-height));
  font-size: var(--%NS%mat-card-subtitle-text-size, var(--%NS%mat-sys-title-medium-size));
  letter-spacing: var(--%NS%mat-card-subtitle-text-tracking, var(--%NS%mat-sys-title-medium-tracking));
  font-weight: var(--%NS%mat-card-subtitle-text-weight, var(--%NS%mat-sys-title-medium-weight));
}

.mat-mdc-card-title,
.mat-mdc-card-subtitle {
  display: block;
  margin: 0;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle {
  padding: 16px 16px 0;
}

.mat-mdc-card-header {
  display: flex;
  padding: 16px 16px 0;
}

.mat-mdc-card-content {
  display: block;
  padding: 0 16px;
}
.mat-mdc-card-content:first-child {
  padding-top: 16px;
}
.mat-mdc-card-content:last-child {
  padding-bottom: 16px;
}

.mat-mdc-card-title-group {
  display: flex;
  justify-content: space-between;
  width: 100%;
}

.mat-mdc-card-avatar {
  height: 40px;
  width: 40px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-bottom: 16px;
  object-fit: cover;
}
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-avatar ~ .mat-mdc-card-header-text .mat-mdc-card-title {
  line-height: normal;
}

.mat-mdc-card-sm-image {
  width: 80px;
  height: 80px;
}

.mat-mdc-card-md-image {
  width: 112px;
  height: 112px;
}

.mat-mdc-card-lg-image {
  width: 152px;
  height: 152px;
}

.mat-mdc-card-xl-image {
  width: 240px;
  height: 240px;
}

.mat-mdc-card-subtitle ~ .mat-mdc-card-title,
.mat-mdc-card-title ~ .mat-mdc-card-subtitle,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-title,
.mat-mdc-card-header .mat-mdc-card-header-text .mat-mdc-card-subtitle,
.mat-mdc-card-title-group .mat-mdc-card-title,
.mat-mdc-card-title-group .mat-mdc-card-subtitle {
  padding-top: 0;
}

.mat-mdc-card-content > :last-child:not(.mat-mdc-card-footer) {
  margin-bottom: 0;
}

.mat-mdc-card-actions-align-end {
  justify-content: flex-end;
}
`],encapsulation:2})})()}return t})(),rn=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["mat-card-title"],["","mat-card-title",""],["","matCardTitle",""]],hostAttrs:[1,"mat-mdc-card-title"]})}return t})();var Rt=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["mat-card-content"]],hostAttrs:[1,"mat-mdc-card-content"]})}return t})();var on=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=[[["","mat-card-avatar",""],["","matCardAvatar",""]],[["mat-card-title"],["mat-card-subtitle"],["","mat-card-title",""],["","mat-card-subtitle",""],["","matCardTitle",""],["","matCardSubtitle",""]],"*"];return k({type:t,selectors:[["mat-card-header"]],hostAttrs:[1,"mat-mdc-card-header"],ngContentSelectors:["[mat-card-avatar], [matCardAvatar]",`mat-card-title, mat-card-subtitle,
      [mat-card-title], [mat-card-subtitle],
      [matCardTitle], [matCardSubtitle]`,"*"],decls:4,vars:0,consts:[[1,"mat-mdc-card-header-text"]],template:function(o,a){o&1&&(Oe(e),ce(0),Ne(1,"div",0),ce(2,1),Re(),ce(3,2))},encapsulation:2})})()}return t})();var an=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Fe]})}return t})();function oR(t,n){if(t&1){let e=xr();v(0,"p")(1,"mat-checkbox",4),B("change",function(){let i=dt(e).$implicit,o=fe();return ut(o.toggleTopping(i))}),M(2),mo(3,"currency"),y()()}if(t&2){let e=n.$implicit;_(),z("checked",e.checked),_(),vi(" ",e.name," (",fo(3,3,e.price),") ")}}var $d=class t{pizzaSvc=u(wd);availablePizzaToppings=A(this.pizzaSvc.getAvailablePizzaToppings());total=ft(()=>this.availablePizzaToppings().filter(n=>n.checked).reduce((n,e)=>n+e.price,0));toggleTopping=n=>this.availablePizzaToppings.update(e=>e.map(r=>r===n?R(g({},r),{checked:!r.checked}):r));checkAll=()=>this.availablePizzaToppings.update(n=>n.map(e=>R(g({},e),{checked:!0})));uncheckAll=()=>this.availablePizzaToppings.update(n=>n.map(e=>R(g({},e),{checked:!1})));static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-pizza-toppings"]],decls:16,vars:3,consts:[["appearance","outlined"],[2,"margin-bottom","16px"],["mat-flat-button","",3,"click"],["mat-stroked-button","",3,"click"],[3,"change","checked"]],template:function(e,r){e&1&&(v(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),M(3," Choose your pizza toppings... "),y()(),v(4,"mat-card-content")(5,"div",1)(6,"button",2),B("click",function(){return r.checkAll()}),M(7," All "),y(),M(8," \xA0 "),v(9,"button",3),B("click",function(){return r.uncheckAll()}),M(10," None "),y()(),pi(11,oR,4,5,"p",null,dp),v(13,"h3"),M(14),mo(15,"currency"),y()()()),e&2&&(_(11),hi(r.availablePizzaToppings()),_(3),St(" Total: ",fo(15,1,r.total())," "))},dependencies:[Bn,nr,Ud,Yh,an,At,Rt,on,rn,rs],styles:["mat-card-header[_ngcontent-%COMP%]{margin-bottom:20px}"]})};var Rs=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new E;bulk={select:n=>this._select(n),deselect:n=>this._deselect(n),setSelection:n=>this._setSelection(n)};constructor(n=!1,e,r=!0,i){this._multiple=n,this._emitChanges=r,this.compareWith=i,e&&e.length&&(n?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...n){return this._select(n)}deselect(...n){return this._deselect(n)}setSelection(...n){return this._setSelection(n)}toggle(n){return this.isSelected(n)?this.deselect(n):this.select(n)}clear(n=!0){this._unmarkAll();let e=this._hasQueuedChanges();return n&&this._emitChangeEvent(),e}isSelected(n){return this._selection.has(this._getConcreteValue(n))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(n){this._multiple&&this.selected&&this._selected.sort(n)}isMultipleSelection(){return this._multiple}_select(n){this._verifyValueAssignment(n),n.forEach(r=>this._markSelected(r));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_deselect(n){this._verifyValueAssignment(n),n.forEach(r=>this._unmarkSelected(r));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}_setSelection(n){this._verifyValueAssignment(n);let e=this.selected,r=new Set(n.map(o=>this._getConcreteValue(o)));n.forEach(o=>this._markSelected(o)),e.filter(o=>!r.has(this._getConcreteValue(o,r))).forEach(o=>this._unmarkSelected(o));let i=this._hasQueuedChanges();return this._emitChangeEvent(),i}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(n){n=this._getConcreteValue(n),this.isSelected(n)||(this._multiple||this._unmarkAll(),this.isSelected(n)||this._selection.add(n),this._emitChanges&&this._selectedToEmit.push(n))}_unmarkSelected(n){n=this._getConcreteValue(n),this.isSelected(n)&&(this._selection.delete(n),this._emitChanges&&this._deselectedToEmit.push(n))}_unmarkAll(){this.isEmpty()||this._selection.forEach(n=>this._unmarkSelected(n))}_verifyValueAssignment(n){n.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(n,e){if(this.compareWith){e=e??this._selection;for(let r of e)if(this.compareWith(n,r))return r;return n}else return n}};var Kh=(()=>{class t{_listeners=[];notify(e,r){for(let i of this._listeners)i(e,r)}listen(e){return this._listeners.push(e),()=>{this._listeners=this._listeners.filter(r=>e!==r)}}ngOnDestroy(){this._listeners=[]}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var Gd=class{source;value;constructor(n,e){this.source=n,this.value=e}},aR={provide:wn,useExisting:$e(()=>Zh),multi:!0},DC=new b("MatRadioGroup"),sR=new b("mat-radio-default-options",{providedIn:"root",factory:()=>({color:"accent",disabledInteractive:!1})}),Zh=(()=>{class t{_changeDetector=u(ve);_value=null;_name=u(je).getId("mat-radio-group-");_selected=null;_isInitialized=!1;_labelPosition="after";_disabled=!1;_required=!1;_buttonChanges;_controlValueAccessorChangeFn=()=>{};onTouched=()=>{};change=new C;_radios;color;get name(){return this._name}set name(e){this._name=e,this._updateRadioButtonNames()}get labelPosition(){return this._labelPosition}set labelPosition(e){this._labelPosition=e==="before"?"before":"after",this._markRadiosForCheck()}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this._updateSelectedRadioFromValue(),this._checkSelectedRadioButton())}_checkSelectedRadioButton(){this._selected&&!this._selected.checked&&(this._selected.checked=!0)}get selected(){return this._selected}set selected(e){this._selected=e,this.value=e?e.value:null,this._checkSelectedRadioButton()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markRadiosForCheck()}get required(){return this._required}set required(e){this._required=e,this._markRadiosForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markRadiosForCheck()}_disabledInteractive=!1;ngAfterContentInit(){this._isInitialized=!0,this._buttonChanges=this._radios.changes.subscribe(()=>{this.selected&&!this._radios.find(e=>e===this.selected)&&(this._selected=null)})}ngOnDestroy(){this._buttonChanges?.unsubscribe()}_touch(){this.onTouched&&this.onTouched()}_updateRadioButtonNames(){this._radios&&this._radios.forEach(e=>{e.name=this.name,e._markForCheck()})}_updateSelectedRadioFromValue(){let e=this._selected!==null&&this._selected.value===this._value;this._radios&&!e&&(this._selected=null,this._radios.forEach(r=>{r.checked=this.value===r.value,r.checked&&(this._selected=r)}))}_emitChangeEvent(){this._isInitialized&&this.change.emit(new Gd(this._selected,this._value))}_markRadiosForCheck(){this._radios&&this._radios.forEach(e=>e._markForCheck())}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this.onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetector.markForCheck()}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["mat-radio-group"]],contentQueries:function(r,i,o){if(r&1&&Ln(o,Wd,5),r&2){let a;ae(a=se())&&(i._radios=a)}},hostAttrs:["role","radiogroup",1,"mat-mdc-radio-group"],inputs:{color:"color",name:"name",labelPosition:"labelPosition",value:"value",selected:"selected",disabled:[2,"disabled","disabled",N],required:[2,"required","required",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},outputs:{change:"change"},exportAs:["matRadioGroup"],features:[ke([aR,{provide:DC,useExisting:t}])]})}return t})(),Wd=(()=>{class t{_elementRef=u(V);_changeDetector=u(ve);_focusMonitor=u(en);_radioDispatcher=u(Kh);_defaultOptions=u(sR,{optional:!0});_ngZone=u(T);_renderer=u(Te);_uniqueId=u(je).getId("mat-radio-");_cleanupClick;id=this._uniqueId;name;ariaLabel;ariaLabelledby;ariaDescribedby;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked!==e&&(this._checked=e,e&&this.radioGroup&&this.radioGroup.value!==this.value?this.radioGroup.selected=this:!e&&this.radioGroup&&this.radioGroup.value===this.value&&(this.radioGroup.selected=null),e&&this._radioDispatcher.notify(this.id,this.name),this._changeDetector.markForCheck())}get value(){return this._value}set value(e){this._value!==e&&(this._value=e,this.radioGroup!==null&&(this.checked||(this.checked=this.radioGroup.value===e),this.checked&&(this.radioGroup.selected=this)))}get labelPosition(){return this._labelPosition||this.radioGroup&&this.radioGroup.labelPosition||"after"}set labelPosition(e){this._labelPosition=e}_labelPosition;get disabled(){return this._disabled||this.radioGroup!==null&&this.radioGroup.disabled}set disabled(e){this._setDisabled(e)}get required(){return this._required||this.radioGroup&&this.radioGroup.required}set required(e){e!==this._required&&this._changeDetector.markForCheck(),this._required=e}get color(){return this._color||this.radioGroup&&this.radioGroup.color||this._defaultOptions&&this._defaultOptions.color||"accent"}set color(e){this._color=e}_color;get disabledInteractive(){return this._disabledInteractive||this.radioGroup!==null&&this.radioGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new C;radioGroup;get inputId(){return`${this.id||this._uniqueId}-input`}_checked=!1;_disabled=!1;_required=!1;_value=null;_removeUniqueSelectionListener=()=>{};_previousTabIndex;_inputElement;_rippleTrigger;_noopAnimations=Be();_injector=u(J);constructor(){u(Me).load(nn);let e=u(DC,{optional:!0}),r=u(new jt("tabindex"),{optional:!0});this.radioGroup=e,this._disabledInteractive=this._defaultOptions?.disabledInteractive??!1,r&&(this.tabIndex=er(r,0))}focus(e,r){r?this._focusMonitor.focusVia(this._inputElement,r,e):this._inputElement.nativeElement.focus(e)}_markForCheck(){this._changeDetector.markForCheck()}ngOnInit(){this.radioGroup&&(this.checked=this.radioGroup.value===this._value,this.checked&&(this.radioGroup.selected=this),this.name=this.radioGroup.name),this._removeUniqueSelectionListener=this._radioDispatcher.listen((e,r)=>{e!==this.id&&r===this.name&&(this.checked=!1)})}ngDoCheck(){this._updateTabIndex()}ngAfterViewInit(){this._updateTabIndex(),this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{!e&&this.radioGroup&&this.radioGroup._touch()}),this._ngZone.runOutsideAngular(()=>{this._cleanupClick=this._renderer.listen(this._inputElement.nativeElement,"click",this._onInputClick)})}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._removeUniqueSelectionListener()}_emitChangeEvent(){this.change.emit(new Gd(this,this._value))}_isRippleDisabled(){return this.disableRipple||this.disabled}_onInputInteraction(e){if(e.stopPropagation(),!this.checked&&!this.disabled){let r=this.radioGroup&&this.value!==this.radioGroup.value;this.checked=!0,this._emitChangeEvent(),this.radioGroup&&(this.radioGroup._controlValueAccessorChangeFn(this.value),r&&this.radioGroup._emitChangeEvent())}}_setDisabled(e){this._disabled!==e&&(this._disabled=e,this._changeDetector.markForCheck())}_onInputClick=e=>{this.disabled&&this.disabledInteractive&&e.preventDefault()};_updateTabIndex(){let e=this.radioGroup,r;if(!e||!e.selected||this.disabled?r=this.tabIndex:r=e.selected===this?this.tabIndex:-1,r!==this._previousTabIndex){let i=this._inputElement?.nativeElement;i&&(i.setAttribute("tabindex",r+""),this._previousTabIndex=r,it(()=>{queueMicrotask(()=>{e&&e.selected&&e.selected!==this&&document.activeElement===i&&(e.selected?._inputElement.nativeElement.focus(),document.activeElement===i&&this._inputElement.nativeElement.blur())})},{injector:this._injector}))}}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["input"],r=["formField"];return k({type:t,selectors:[["mat-radio-button"]],viewQuery:function(a,s){if(a&1&&He(e,5)(r,7,V),a&2){let l;ae(l=se())&&(s._inputElement=l.first),ae(l=se())&&(s._rippleTrigger=l.first)}},hostAttrs:[1,"mat-mdc-radio-button"],hostVars:19,hostBindings:function(a,s){a&1&&B("focus",function(){return s._inputElement.nativeElement.focus()}),a&2&&(de("id",s.id)("tabindex",null)("aria-label",null)("aria-labelledby",null)("aria-describedby",null),U("mat-primary",s.color==="primary")("mat-accent",s.color==="accent")("mat-warn",s.color==="warn")("mat-mdc-radio-checked",s.checked)("mat-mdc-radio-disabled",s.disabled)("mat-mdc-radio-disabled-interactive",s.disabledInteractive)("_mat-animation-noopable",s._noopAnimations))},inputs:{id:"id",name:"name",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],disableRipple:[2,"disableRipple","disableRipple",N],tabIndex:[2,"tabIndex","tabIndex",o=>o==null?0:er(o)],checked:[2,"checked","checked",N],value:"value",labelPosition:"labelPosition",disabled:[2,"disabled","disabled",N],required:[2,"required","required",N],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},outputs:{change:"change"},exportAs:["matRadioButton"],ngContentSelectors:["*"],decls:13,vars:17,consts:[["formField",""],["input",""],["mat-internal-form-field","",3,"labelPosition","for"],[1,"mdc-radio"],[1,"mat-mdc-radio-touch-target"],["type","radio","aria-invalid","false",1,"mdc-radio__native-control",3,"change","id","checked","disabled","required"],["aria-hidden","true",1,"mdc-radio__background"],[1,"mdc-radio__outer-circle"],[1,"mdc-radio__inner-circle"],["mat-ripple","","aria-hidden","true",1,"mat-radio-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mat-ripple-element","mat-radio-persistent-ripple"],[1,"mat-internal-form-field-label","mdc-label"]],template:function(a,s){a&1&&(Oe(),v(0,"label",2,0)(2,"span",3),Z(3,"span",4),v(4,"input",5,1),B("change",function(c){return s._onInputInteraction(c)}),y(),v(6,"span",6),Z(7,"span",7)(8,"span",8),y(),v(9,"span",9),Z(10,"span",10),y()(),v(11,"span",11),ce(12),y()()),a&2&&(z("labelPosition",s.labelPosition)("for",s.inputId),_(2),U("mdc-radio--disabled",s.disabled),_(2),z("id",s.inputId)("checked",s.checked)("disabled",s.disabled&&!s.disabledInteractive)("required",s.required),de("name",s.name)("value",s.value)("aria-label",s.ariaLabel)("aria-labelledby",s.ariaLabelledby)("aria-describedby",s.ariaDescribedby)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),_(5),z("matRippleTrigger",s._rippleTrigger.nativeElement)("matRippleDisabled",s._isRippleDisabled())("matRippleCentered",!0))},dependencies:[Mr,jo],styles:[`.mat-mdc-radio-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-radio-button .mdc-radio {
  display: inline-block;
  position: relative;
  flex: 0 0 auto;
  box-sizing: content-box;
  width: 20px;
  height: 20px;
  will-change: opacity, transform, border-color, color;
  padding: calc((var(--%NS%mat-radio-state-layer-size, 40px) - 20px) / 2);
  cursor: pointer;
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]):not(:focus) ~ .mdc-radio__background::before {
  opacity: 0.04;
  transform: scale(1);
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:not([disabled]) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-hover-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-hover-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:hover > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-hover-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-pressed-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-pressed-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio:active > .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-pressed-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__background {
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  width: 20px;
  height: 20px;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  position: absolute;
  transform: scale(0, 0);
  border-radius: 50%;
  opacity: 0;
  pointer-events: none;
  content: "";
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  width: var(--%NS%mat-radio-state-layer-size, 40px);
  height: var(--%NS%mat-radio-state-layer-size, 40px);
  top: calc(-1 * (var(--%NS%mat-radio-state-layer-size, 40px) - 20px) / 2);
  left: calc(-1 * (var(--%NS%mat-radio-state-layer-size, 40px) - 20px) / 2);
}
.mat-mdc-radio-button .mdc-radio__outer-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  border-width: 2px;
  border-style: solid;
  border-radius: 50%;
  transition: border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-radio-button .mdc-radio__inner-circle {
  position: absolute;
  top: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  transform: scale(0);
  border-radius: 50%;
  transition: transform 90ms cubic-bezier(0.4, 0, 0.6, 1), background-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
@media (forced-colors: active) {
  .mat-mdc-radio-button .mdc-radio__inner-circle {
    background-color: CanvasText !important;
  }
}
.mat-mdc-radio-button .mdc-radio__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  top: 0;
  right: 0;
  left: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--%NS%mat-radio-state-layer-size, 40px);
  height: var(--%NS%mat-radio-state-layer-size, 40px);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle, .mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:focus + .mdc-radio__background::before {
  transform: scale(1);
  opacity: 0.12;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 1), transform 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-unselected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background {
  cursor: default;
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:disabled + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface, currentColor));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-selected-focus-icon-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button .mdc-radio__native-control:enabled:focus:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-selected-focus-icon-color, var(--%NS%mat-sys-primary, currentColor));
}
.mat-mdc-radio-button .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle {
  transform: scale(0.5);
  transition: transform 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled {
  pointer-events: auto;
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:not(:checked) + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-unselected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-unselected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--%NS%disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__outer-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--%NS%disabled:hover .mdc-radio__native-control:checked + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control:checked:focus + .mdc-radio__background > .mdc-radio__inner-circle,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__native-control + .mdc-radio__background > .mdc-radio__inner-circle {
  background-color: var(--%NS%mat-radio-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface, currentColor));
  opacity: var(--%NS%mat-radio-disabled-selected-icon-opacity, 0.38);
}
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__background::before,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__outer-circle,
.mat-mdc-radio-button._mat-animation-noopable .mdc-radio__inner-circle {
  transition: none !important;
}
.mat-mdc-radio-button .mat-internal-form-field-label:empty {
  display: none;
}
.mat-mdc-radio-button .mdc-radio__background::before {
  background-color: var(--%NS%mat-radio-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button.mat-mdc-radio-checked .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-checked .mdc-radio__background::before {
  background-color: var(--%NS%mat-radio-checked-ripple-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mat-ripple-element,
.mat-mdc-radio-button.mat-mdc-radio-disabled-interactive .mdc-radio--disabled .mdc-radio__background::before {
  background-color: var(--%NS%mat-radio-ripple-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button .mat-internal-form-field {
  color: var(--%NS%mat-radio-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-radio-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-radio-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-radio-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-radio-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-radio-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
  cursor: pointer;
}
.mat-mdc-radio-button .mdc-radio--disabled + .mat-internal-form-field-label {
  color: var(--%NS%mat-radio-disabled-label-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-radio-button .mat-radio-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: 50%;
}
.mat-mdc-radio-button .mat-radio-ripple > .mat-ripple-element {
  opacity: 0.14;
}
.mat-mdc-radio-button .mat-radio-ripple::before {
  border-radius: 50%;
}
.mat-mdc-radio-button .mdc-radio > .mdc-radio__native-control:focus:enabled:not(:checked) ~ .mdc-radio__background > .mdc-radio__outer-circle {
  border-color: var(--%NS%mat-radio-unselected-focus-icon-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-radio-button.cdk-focused .mat-focus-indicator::before {
  content: "";
}

.mat-mdc-radio-disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-radio-disabled.mat-mdc-radio-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-radio-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-radio-touch-target-size, 48px);
  width: var(--%NS%mat-radio-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-radio-touch-target-display, block);
}
[dir=rtl] .mat-mdc-radio-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2})})()}return t})(),SC=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Fo,Wd,Fe]})}return t})();var qd=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-leave-tip"]],decls:14,vars:0,consts:[["appearance","outlined"],["aria-label","Select an option"],["value","1"],["value","2"],["value","3"],["value","4"]],template:function(e,r){e&1&&(v(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),M(3," Leave a tip "),y()(),v(4,"mat-card-content")(5,"mat-radio-group",1)(6,"mat-radio-button",2),M(7,"NO TIP"),y(),v(8,"mat-radio-button",3),M(9,"15%"),y(),v(10,"mat-radio-button",4),M(11,"20%"),y(),v(12,"mat-radio-button",5),M(13,"25%"),y()()()())},dependencies:[an,At,Rt,on,rn,SC,Zh,Wd],styles:["mat-card[_ngcontent-%COMP%]{margin-top:2em}mat-card-header[_ngcontent-%COMP%]{margin-bottom:2em}mat-radio-button[_ngcontent-%COMP%]{display:block}"]})};var kC=Symbol("FIELD_TREE");var IC=Symbol("IS_ASYNC_VALIDATION_RESOURCE"),EC=class{reducer;create;brand;[IC];constructor(n,e){this.reducer=n,this.create=e}};function Os(t){return typeof t=="function"&&t[kC]===!0}var MC=new b("");var Qh=class{_box;_destroyed=new E;_resizeSubject=new E;_resizeObserver;_elementObservables=new Map;constructor(n){this._box=n,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(n){return this._elementObservables.has(n)||this._elementObservables.set(n,new q(e=>{let r=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(n,{box:this._box}),()=>{this._resizeObserver?.unobserve(n),r.unsubscribe(),this._elementObservables.delete(n)}}).pipe(Ye(e=>e.some(r=>r.target===n)),Al({bufferSize:1,refCount:!0}),Ue(this._destroyed))),this._elementObservables.get(n)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},NC=(()=>{class t{_cleanupErrorListener;_observers=new Map;_ngZone=u(T);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,r){let i=r?.box||"content-box";return this._observers.has(i)||this._observers.set(i,new Qh(i)),this._observers.get(i).observe(e)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var ir=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["mat-label"]]})}return t})(),cR=new b("MatError");var Fs=(()=>{class t{align="start";id=u(je).getId("mat-mdc-hint-");static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(r,i){r&2&&(mt("id",i.id),de("align",null),U("mat-mdc-form-field-hint-end",i.align==="end"))},inputs:{align:"align",id:"id"}})}return t})(),dR=new b("MatPrefix");var LC=new b("MatSuffix"),Xh=(()=>{class t{set _isTextSelector(e){this._isText=!0}_isText=!1;static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["","matSuffix",""],["","matIconSuffix",""],["","matTextSuffix",""]],inputs:{_isTextSelector:[0,"matTextSuffix","_isTextSelector"]},features:[ke([{provide:LC,useExisting:t}])]})}return t})(),VC=new b("FloatingLabelParent"),TC=(()=>{class t{_elementRef=u(V);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(NC);_ngZone=u(T);_parent=u(VC);_resizeSubscription=new ue;ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return uR(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(r,i){r&2&&U("mdc-floating-label--float-above",i.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return t})();function uR(t){let n=t;if(n.offsetParent!==null)return n.scrollWidth;let e=n.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let r=e.scrollWidth;return e.remove(),r}var AC="mdc-line-ripple--active",Yd="mdc-line-ripple--deactivating",RC=(()=>{class t{_elementRef=u(V);_cleanupTransitionEnd;constructor(){let e=u(T),r=u(Te);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=r.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Yd),e.add(AC)}deactivate(){this._elementRef.nativeElement.classList.add(Yd)}_handleTransitionEnd=e=>{let r=this._elementRef.nativeElement.classList,i=r.contains(Yd);e.propertyName==="opacity"&&i&&r.remove(AC,Yd)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return t})(),OC=(()=>{class t{_elementRef=u(V);_ngZone=u(T);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,r=e.querySelector(".mdc-floating-label");r?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(r.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>r.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let r=this._notch.nativeElement;!this.open||!e?r.style.width="":r.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["notch"];return k({type:t,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(o,a){if(o&1&&He(e,5),o&2){let s;ae(s=se())&&(a._notch=s.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(o,a){o&2&&U("mdc-notched-outline--notched",a.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},ngContentSelectors:["*"],decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(o,a){o&1&&(Oe(),Mt(0,"div",1),Ne(1,"div",2,0),ce(3),Re(),Mt(4,"div",3))},encapsulation:2})})()}return t})(),Kd=(()=>{class t{id;ngField=null;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;stateChanges=null;value;static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t})}return t})();var Ps=new b("MatFormField"),mR=new b("MAT_FORM_FIELD_DEFAULT_OPTIONS"),FC="fill",fR="auto",PC="fixed",pR="translateY(-50%)",Tr=(()=>{class t{_elementRef=u(V);_changeDetectorRef=u(ve);_platform=u(he);_idGenerator=u(je);_ngZone=u(T);_defaults=u(mR,{optional:!0});_currentDirection;_unwrapMaybeSignal(e){return Tn(e)?e():e}_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=es("iconPrefixContainer");_textPrefixContainerSignal=es("textPrefixContainer");_iconSuffixContainerSignal=es("iconSuffixContainer");_textSuffixContainerSignal=es("textSuffixContainer");_prefixSuffixContainers=ft(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=K_(ir);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=tr(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||fR}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let r=e||this._defaults?.appearance||FC;this._appearanceSignal.set(r)}_appearanceSignal=A(FC);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||PC}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||PC}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new E;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Be();constructor(){let e=this._defaults,r=u(Tt);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Nn(()=>this._currentDirection=r.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=Os(this._control.ngField)?null:this._control.ngControl.control.validator),this._previousControl=this._control,this._changeDetectorRef.markForCheck()),this._control.ngControl&&this._control.ngControl.control&&!Os(this._control.ngField)&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=ft(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let r=this._control,i="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(i+e.controlType),r.controlType&&this._elementRef.nativeElement.classList.add(i+r.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=r.stateChanges?.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=r.stateChanges?.pipe(Ht([void 0,void 0]),_e(()=>[this._unwrapMaybeSignal(r.errorState),r.userAriaDescribedBy]),Tl(),Ye(([[o,a],[s,l]])=>o!==s||a!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),r.ngControl&&r.ngControl.valueChanges&&!Os(r.ngField)&&(this._valueChanges=r.ngControl.valueChanges.pipe(Ue(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Wr(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._unwrapMaybeSignal(this._control.focused);e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){vp({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=ft(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._shouldAlwaysFloat()||this._unwrapMaybeSignal(this._control.shouldLabelFloat):!1}_shouldForward(e){let r=this._control?.ngField||this._control?.ngControl;if(!r)return!1;if(Os(r)){let i=r();return e==="valid"?i.valid():e==="dirty"?i.dirty():e==="touched"?i.touched():e==="pending"?i.pending():e==="untouched"?!i.touched():e==="pristine"?!i.dirty():e==="invalid"?!i.valid():!1}else{let i=r;return e==="valid"?i.valid:e==="dirty"?i.dirty:e==="touched"?i.touched:e==="pending"?i.pending:e==="untouched"?i.untouched:e==="pristine"?i.pristine:e==="invalid"?i.invalid:!1}}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._unwrapMaybeSignal(this._control.errorState)?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(s=>s.align==="start"):null,a=this._hintChildren?this._hintChildren.find(s=>s.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),a&&e.push(a.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let r=this._control.describedByIds,i;if(r){let o=this._describedByIds||e;i=e.concat(r.filter(a=>a&&!o.includes(a)))}else i=e;this._control.setDescribedByIds(i),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,r=this._textPrefixContainer?.nativeElement,i=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,a=e?.getBoundingClientRect().width??0,s=r?.getBoundingClientRect().width??0,l=i?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",m=`${a+s}px`,f=`calc(${d} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,h=`var(--mat-mdc-form-field-label-transform, ${pR} translateX(${f}))`,w=a+s+l+c;return[h,w]}_writeOutlinedLabelStyles(e){if(e!==null){let[r,i]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=r),i!==null&&this._notchedOutline?._setMaxWidth(i)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let r=e.getRootNode();return r&&r!==e}return document.documentElement.contains(e)}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["iconPrefixContainer"],r=["textPrefixContainer"],i=["iconSuffixContainer"],o=["textSuffixContainer"],a=["textField"],s=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],l=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function c(te,Ce){te&1&&Z(0,"span",21)}function d(te,Ce){if(te&1&&(v(0,"label",20),ce(1,1),xe(2,c,1,0,"span",21),y()),te&2){let x=fe(2);z("floating",x._shouldLabelFloat())("monitorResize",x._hasOutline())("id",x._labelId),de("for",x._control.disableAutomaticLabeling?null:x._control.id),_(2),we(!x.hideRequiredMarker&&x._unwrapMaybeSignal(x._control.required)?2:-1)}}function m(te,Ce){if(te&1&&xe(0,d,3,5,"label",20),te&2){let x=fe();we(x._hasFloatingLabel()?0:-1)}}function p(te,Ce){te&1&&Z(0,"div",7)}function f(te,Ce){}function h(te,Ce){if(te&1&&Jn(0,f,0,0,"ng-template",13),te&2){fe(2);let x=Zt(1);z("ngTemplateOutlet",x)}}function w(te,Ce){if(te&1&&(v(0,"div",9),xe(1,h,1,1,null,13),y()),te&2){let x=fe();z("matFormFieldNotchedOutlineOpen",x._shouldLabelFloat()),_(),we(x._forceDisplayInfixLabel()?-1:1)}}function I(te,Ce){te&1&&(v(0,"div",10,2),ce(2,2),y())}function P(te,Ce){te&1&&(v(0,"div",11,3),ce(2,3),y())}function $(te,Ce){}function Je(te,Ce){if(te&1&&Jn(0,$,0,0,"ng-template",13),te&2){fe();let x=Zt(1);z("ngTemplateOutlet",x)}}function na(te,Ce){te&1&&(v(0,"div",14,4),ce(2,4),y())}function $s(te,Ce){te&1&&(v(0,"div",15,5),ce(2,5),y())}function ra(te,Ce){te&1&&Z(0,"div",16)}function kD(te,Ce){te&1&&(v(0,"div",18),ce(1,6),y())}function ID(te,Ce){if(te&1&&(v(0,"mat-hint",22),M(1),y()),te&2){let x=fe(2);z("id",x._hintLabelId),_(),Qt(x.hintLabel)}}function MD(te,Ce){if(te&1&&(v(0,"div",19),xe(1,ID,2,2,"mat-hint",22),ce(2,7),Z(3,"div",23),ce(4,8),y()),te&2){let x=fe();_(),we(x.hintLabel?1:-1)}}return k({type:t,selectors:[["mat-form-field"]],contentQueries:function(Ce,x,De){if(Ce&1&&(Hc(De,x._labelChild,ir,5),Ln(De,Kd,5)(De,dR,5)(De,LC,5)(De,cR,5)(De,Fs,5)),Ce&2){$c();let xt;ae(xt=se())&&(x._formFieldControl=xt.first),ae(xt=se())&&(x._prefixChildren=xt),ae(xt=se())&&(x._suffixChildren=xt),ae(xt=se())&&(x._errorChildren=xt),ae(xt=se())&&(x._hintChildren=xt)}},viewQuery:function(Ce,x){if(Ce&1&&(Uc(x._iconPrefixContainerSignal,e,5)(x._textPrefixContainerSignal,r,5)(x._iconSuffixContainerSignal,i,5)(x._textSuffixContainerSignal,o,5),He(a,5)(e,5)(r,5)(i,5)(o,5)(TC,5)(OC,5)(RC,5)),Ce&2){$c(4);let De;ae(De=se())&&(x._textField=De.first),ae(De=se())&&(x._iconPrefixContainer=De.first),ae(De=se())&&(x._textPrefixContainer=De.first),ae(De=se())&&(x._iconSuffixContainer=De.first),ae(De=se())&&(x._textSuffixContainer=De.first),ae(De=se())&&(x._floatingLabel=De.first),ae(De=se())&&(x._notchedOutline=De.first),ae(De=se())&&(x._lineRipple=De.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(Ce,x){Ce&2&&U("mat-mdc-form-field-label-always-float",x._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",x._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",x._hasIconSuffix)("mat-form-field-invalid",x._unwrapMaybeSignal(x._control.errorState))("mat-form-field-disabled",x._unwrapMaybeSignal(x._control.disabled))("mat-form-field-autofilled",x._unwrapMaybeSignal(x._control.autofilled))("mat-form-field-appearance-fill",x.appearance=="fill")("mat-form-field-appearance-outline",x.appearance=="outline")("mat-form-field-hide-placeholder",x._hasFloatingLabel()&&!x._shouldLabelFloat())("mat-primary",x.color!=="accent"&&x.color!=="warn")("mat-accent",x.color==="accent")("mat-warn",x.color==="warn")("ng-untouched",x._shouldForward("untouched"))("ng-touched",x._shouldForward("touched"))("ng-pristine",x._shouldForward("pristine"))("ng-dirty",x._shouldForward("dirty"))("ng-valid",x._shouldForward("valid"))("ng-invalid",x._shouldForward("invalid"))("ng-pending",x._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[ke([{provide:Ps,useExisting:t},{provide:VC,useExisting:t}])],ngContentSelectors:l,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(Ce,x){if(Ce&1&&(Oe(s),Jn(0,m,1,1,"ng-template",null,0,up),v(2,"div",6,1),B("click",function(xt){return x._control.onContainerClick(xt)}),xe(4,p,1,0,"div",7),v(5,"div",8),xe(6,w,2,2,"div",9),xe(7,I,3,0,"div",10),xe(8,P,3,0,"div",11),v(9,"div",12),xe(10,Je,1,1,null,13),ce(11),y(),xe(12,na,3,0,"div",14),xe(13,$s,3,0,"div",15),y(),xe(14,ra,1,0,"div",16),y(),v(15,"div",17),xe(16,kD,2,0,"div",18)(17,MD,5,1,"div",19),y()),Ce&2){let De,xt=x._unwrapMaybeSignal(x._control.disabled);_(2),U("mdc-text-field--filled",!x._hasOutline())("mdc-text-field--outlined",x._hasOutline())("mdc-text-field--no-label",!x._hasFloatingLabel())("mdc-text-field--disabled",xt)("mdc-text-field--invalid",x._unwrapMaybeSignal(x._control.errorState)),_(2),we(!x._hasOutline()&&!xt?4:-1),_(2),we(x._hasOutline()?6:-1),_(),we(x._hasIconPrefix?7:-1),_(),we(x._hasTextPrefix?8:-1),_(2),we(!x._hasOutline()||x._forceDisplayInfixLabel()?10:-1),_(2),we(x._hasTextSuffix?12:-1),_(),we(x._hasIconSuffix?13:-1),_(),we(x._hasOutline()?-1:14),_(),U("mat-mdc-form-field-subscript-dynamic-size",x.subscriptSizing==="dynamic");let ND=x._getSubscriptMessageType();_(),we((De=ND)==="error"?16:De==="hint"?17:-1)}},dependencies:[TC,OC,Cp,RC,Fs],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--%NS%disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-filled-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-filled-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-input-text-color, var(--%NS%mat-sys-on-surface));
  caret-color: var(--%NS%mat-form-field-outlined-caret-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-outlined-input-text-placeholder-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-filled-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--%NS%mat-form-field-outlined-error-caret-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--%NS%mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-top-right-radius: var(--%NS%mat-form-field-filled-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) {
  background-color: var(--%NS%mat-form-field-filled-container-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--%NS%mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-hover-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-filled-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-filled-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-filled-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-filled-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-filled-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-label-text-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-focus-label-text-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-hover-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-focus-label-text-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--%NS%mat-form-field-outlined-error-hover-label-text-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--%NS%mat-form-field-outlined-label-text-font, var(--%NS%mat-sys-body-large-font));
  font-size: var(--%NS%mat-form-field-outlined-label-text-size, var(--%NS%mat-sys-body-large-size));
  font-weight: var(--%NS%mat-form-field-outlined-label-text-weight, var(--%NS%mat-sys-body-large-weight));
  letter-spacing: var(--%NS%mat-form-field-outlined-label-text-tracking, var(--%NS%mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--%NS%required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-outline-color, var(--%NS%mat-sys-outline));
  border-width: var(--%NS%mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-hover-outline-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-focus-outline-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-hover-outline-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--%NS%mat-form-field-outlined-error-focus-outline-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--%NS%mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--%NS%mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--%NS%mat-form-field-outlined-container-shape, var(--%NS%mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--%NS%mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-active-indicator-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-hover-active-indicator-color, var(--%NS%mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-active-indicator-color, var(--%NS%mat-sys-error));
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled).mdc-text-field--%NS%invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-hover-active-indicator-color, var(--%NS%mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--%NS%mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--%NS%filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-focus-active-indicator-color, var(--%NS%mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--%NS%invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--%NS%mat-form-field-filled-error-focus-active-indicator-color, var(--%NS%mat-sys-error));
}

.mdc-line-ripple--%NS%active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--%NS%deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--%NS%no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --%NS%mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--%NS%mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--%NS%mat-form-field-container-height, 56px);
  padding-top: var(--%NS%mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--%NS%mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--%NS%mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--%NS%mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--%NS%mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--%NS%mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --%NS%mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--%NS%mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--%NS%mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--%NS%mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--%NS%mat-form-field-error-text-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-subscript-text-font, var(--%NS%mat-sys-body-small-font));
  line-height: var(--%NS%mat-form-field-subscript-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  font-size: var(--%NS%mat-form-field-subscript-text-size, var(--%NS%mat-sys-body-small-size));
  letter-spacing: var(--%NS%mat-form-field-subscript-text-tracking, var(--%NS%mat-sys-body-small-tracking));
  font-weight: var(--%NS%mat-form-field-subscript-text-weight, var(--%NS%mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--%NS%mat-form-field-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--%NS%mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--%NS%mat-form-field-select-option-text-color, var(--%NS%mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--%NS%mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--%NS%mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--%NS%mat-form-field-enabled-select-arrow-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-focus-select-arrow-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--%NS%mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --%NS%mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--%NS%mat-form-field-container-text-font, var(--%NS%mat-sys-body-large-font));
  line-height: var(--%NS%mat-form-field-container-text-line-height, var(--%NS%mat-sys-body-large-line-height));
  font-size: var(--%NS%mat-form-field-container-text-size, var(--%NS%mat-sys-body-large-size));
  letter-spacing: var(--%NS%mat-form-field-container-text-tracking, var(--%NS%mat-sys-body-large-tracking));
  font-weight: var(--%NS%mat-form-field-container-text-weight, var(--%NS%mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--%NS%mat-form-field-outlined-label-text-populated-size) * var(--%NS%mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--%NS%mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-leading-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--%NS%mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-trailing-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-trailing-icon-color, var(--%NS%mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-hover-trailing-icon-color, var(--%NS%mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--%NS%mat-form-field-error-focus-trailing-icon-color, var(--%NS%mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--%NS%filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2})})()}return t})();var or=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Sd,Tr,Fe]})}return t})();var hR=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2})}return t})(),gR={passive:!0},jC=(()=>{class t{_platform=u(he);_ngZone=u(T);_renderer=u(Xe).createRenderer(null,null);_styleLoader=u(Me);_monitoredElements=new Map;monitor(e){if(!this._platform.isBrowser)return qe;this._styleLoader.load(hR);let r=Bt(e),i=this._monitoredElements.get(r);if(i)return i.subject;let o=new E,a="cdk-text-field-autofilled",s=c=>{c.animationName==="cdk-text-field-autofill-start"&&!r.classList.contains(a)?(r.classList.add(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&r.classList.contains(a)&&(r.classList.remove(a),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(r.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(r,"animationstart",s,gR)));return this._monitoredElements.set(r,{subject:o,unlisten:l}),o}stopMonitoring(e){let r=Bt(e),i=this._monitoredElements.get(r);i&&(i.unlisten(),i.subject.complete(),r.classList.remove("cdk-text-field-autofill-monitored"),r.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(r))}ngOnDestroy(){this._monitoredElements.forEach((e,r)=>this.stopMonitoring(r))}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var BC=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({})}return t})();var Zd=new b("MAT_INPUT_VALUE_ACCESSOR");var zC=(()=>{class t{isErrorState(e,r){return!!(e&&e.invalid&&(e.touched||r&&r.submitted))}isSignalErrorState(e){if(!e)return!1;let r=e().invalid(),i=e().touched();return r&&i}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var Qd=class{_defaultMatcher;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;ngControl;formField;constructor(n,e,r,i,o){this._defaultMatcher=n,this._parentFormGroup=r,this._parentForm=i,this._stateChanges=o,e?Tn(e.field)&&!e.updateValueAndValidity?(this.formField=e,this.ngControl=null):(this.formField=null,this.ngControl=e):this.ngControl=this.formField=null}updateErrorState(){let n=this.errorState,e=this._getCurrentErrorState(this.matcher||this._defaultMatcher);e!==n&&(this.errorState=e,this._stateChanges.next())}_getCurrentErrorState(n){if(this.formField&&n?.isSignalErrorState)return n.isSignalErrorState(this.formField.field())??!1;let e=this._parentFormGroup||this._parentForm,r=this.ngControl?this.ngControl.control:null;return n?.isErrorState(r,e)??!1}};var bR=["button","checkbox","file","hidden","image","radio","range","reset","submit"],vR=new b("MAT_INPUT_CONFIG"),zo=(()=>{class t{_elementRef=u(V);_platform=u(he);ngControl=u(As,{optional:!0,self:!0});_autofillMonitor=u(jC);_ngZone=u(T);_formField=u(Ps,{optional:!0});_renderer=u(Te);_uid=u(je).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(vR,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new E;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=tr(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(ki.required)??!1}set required(e){this._required=tr(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&Ah().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=tr(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>Ah().has(e));constructor(){let e=u(zd,{optional:!0}),r=u(Hd,{optional:!0}),i=u(zC),o=u(Zd,{optional:!0,self:!0}),a=u(MC,{optional:!0,self:!0}),s=this._elementRef.nativeElement,l=s.nodeName.toLowerCase();o?Tn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new Qd(i,a||this.ngControl,r,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=l==="select",this._isTextarea=l==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Nn(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let r=this._elementRef.nativeElement;r.type==="number"?(r.type="text",r.setSelectionRange(0,0),r.type="number"):r.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let r=this._elementRef.nativeElement;this._previousPlaceholder=e,e?r.setAttribute("placeholder",e):r.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){bR.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,r=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&r&&r.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let r=this._elementRef.nativeElement;e.length?r.setAttribute("aria-describedby",e.join(" ")):r.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let r=e.target;!r.value&&r.selectionStart===0&&r.selectionEnd===0&&(r.setSelectionRange(1,1),r.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(r,i){r&1&&B("focus",function(){return i._focusChanged(!0)})("blur",function(){return i._focusChanged(!1)})("input",function(){return i._onInput()}),r&2&&(mt("id",i.id)("disabled",i.disabled&&!i.disabledInteractive)("required",i.required),de("name",i.name||null)("readonly",i._getReadonlyAttribute())("aria-disabled",i.disabled&&i.disabledInteractive?"true":null)("aria-invalid",i.empty&&i.required?null:i.errorState)("aria-required",i.required)("id",i.id),U("mat-input-server",i._isServer)("mat-mdc-form-field-textarea-control",i._isInFormField&&i._isTextarea)("mat-mdc-form-field-input-control",i._isInFormField)("mat-mdc-input-disabled-interactive",i.disabledInteractive)("mdc-text-field__input",i._isInFormField)("mat-mdc-native-select-inline",i._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},exportAs:["matInput"],features:[ke([{provide:Kd,useExisting:t}]),Ae]})}return t})(),Ho=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[or,or,BC,Fe]})}return t})();var tg=new b("MAT_DATE_LOCALE",{providedIn:"root",factory:()=>u(yi)}),Uo="Method not implemented",_t=class{locale;_localeChanges=new E;localeChanges=this._localeChanges;setTime(n,e,r,i){throw new Error(Uo)}getHours(n){throw new Error(Uo)}getMinutes(n){throw new Error(Uo)}getSeconds(n){throw new Error(Uo)}parseTime(n,e){throw new Error(Uo)}addSeconds(n,e){throw new Error(Uo)}getValidDateOrNull(n){return this.isDateInstance(n)&&this.isValid(n)?n:null}deserialize(n){return n==null||this.isDateInstance(n)&&this.isValid(n)?n:this.invalid()}setLocale(n){this.locale=n,this._localeChanges.next()}compareDate(n,e){return this.getYear(n)-this.getYear(e)||this.getMonth(n)-this.getMonth(e)||this.getDate(n)-this.getDate(e)}compareTime(n,e){return this.getHours(n)-this.getHours(e)||this.getMinutes(n)-this.getMinutes(e)||this.getSeconds(n)-this.getSeconds(e)}sameDate(n,e){if(n&&e){let r=this.isValid(n),i=this.isValid(e);return r&&i?!this.compareDate(n,e):r==i}return n==e}sameTime(n,e){if(n&&e){let r=this.isValid(n),i=this.isValid(e);return r&&i?!this.compareTime(n,e):r==i}return n==e}clampDate(n,e,r){return e&&this.compareDate(n,e)<0?e:r&&this.compareDate(n,r)>0?r:n}},Ar=new b("mat-date-formats");var yR=20,$o=(()=>{class t{_ngZone=u(T);_platform=u(he);_renderer=u(Xe).createRenderer(null,null);_cleanupGlobalListener;_scrolled=new E;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let r=this.scrollContainers.get(e);r&&(r.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=yR){return this._platform.isBrowser?new q(r=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let i=e>0?this._scrolled.pipe(kl(e)).subscribe(r):this._scrolled.subscribe(r);return this._scrolledCount++,()=>{i.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):Y()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,r)=>this.deregister(r)),this._scrolled.complete()}ancestorScrolled(e,r){let i=this.getAncestorScrollContainers(e);return this.scrolled(r).pipe(Ye(o=>!o||i.indexOf(o)>-1))}getAncestorScrollContainers(e){let r=[];return this.scrollContainers.forEach((i,o)=>{this._targetContainsElement(o,e)&&r.push(o)}),r}_targetContainsElement(e,r){let i=Bt(r),o=e.getElementRef().nativeElement;do if(i==o)return!0;while(i=i.parentElement);return!1}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var _R=20,Go=(()=>{class t{_platform=u(he);_listeners;_viewportSize=null;_change=new E;_document=u(G);constructor(){let e=u(T),r=u(Xe).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let i=o=>this._change.next(o);this._listeners=[r.listen("window","resize",i),r.listen("window","orientationchange",i)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:r,height:i}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+i,right:e.left+r,height:i,width:r}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,r=this._getWindow(),i=e.documentElement,o=i.getBoundingClientRect(),a=-o.top||e.body?.scrollTop||r.scrollY||i.scrollTop||0,s=-o.left||e.body?.scrollLeft||r.scrollX||i.scrollLeft||0;return{top:a,left:s}}change(e=_R){return e>0?this._change.pipe(kl(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var Ls=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({})}return t})(),ng=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Fe,Ls,Fe,Ls]})}return t})();var Vs=class{_attachedHost=null;attach(n){return this._attachedHost=n,n.attach(this)}detach(){let n=this._attachedHost;n!=null&&(this._attachedHost=null,n.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(n){this._attachedHost=n}},Rr=class extends Vs{component;viewContainerRef;injector;projectableNodes;bindings;directives;constructor(n,e,r,i,o,a){super(),this.component=n,this.viewContainerRef=e,this.injector=r,this.projectableNodes=i,this.bindings=o||null,this.directives=a||null}},js=class extends Vs{templateRef;viewContainerRef;context;injector;constructor(n,e,r,i){super(),this.templateRef=n,this.viewContainerRef=e,this.context=r,this.injector=i}get origin(){return this.templateRef.elementRef}attach(n,e=this.context){return this.context=e,super.attach(n)}detach(){return this.context=void 0,super.detach()}},rg=class extends Vs{element;constructor(n){super(),this.element=n instanceof V?n.nativeElement:n}},Xd=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(n){if(n instanceof Rr)return this._attachedPortal=n,this.attachComponentPortal(n);if(n instanceof js)return this._attachedPortal=n,this.attachTemplatePortal(n);if(this.attachDomPortal&&n instanceof rg)return this._attachedPortal=n,this.attachDomPortal(n)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(n){this._disposeFn=n}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Jd=class extends Xd{outletElement;_appRef;_defaultInjector;constructor(n,e,r){super(),this.outletElement=n,this._appRef=e,this._defaultInjector=r}attachComponentPortal(n){let e;if(n.viewContainerRef){let r=n.injector||n.viewContainerRef.injector,i=r.get(Fn,null,{optional:!0})||void 0;e=n.viewContainerRef.createComponent(n.component,{index:n.viewContainerRef.length,injector:r,ngModuleRef:i,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),this.setDisposeFn(()=>e.destroy())}else{let r=this._appRef,i=n.injector||this._defaultInjector||J.NULL,o=i.get(Le,r.injector);e=Wc(n.component,{elementInjector:i,environmentInjector:o,projectableNodes:n.projectableNodes||void 0,bindings:n.bindings||void 0,directives:n.directives||void 0}),r.attachView(e.hostView),this.setDisposeFn(()=>{r.viewCount>0&&r.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=n,e}attachTemplatePortal(n){let e=n.viewContainerRef,r=e.createEmbeddedView(n.templateRef,n.context,{injector:n.injector});return r.rootNodes.forEach(i=>this.outletElement.appendChild(i)),r.detectChanges(),this.setDisposeFn(()=>{let i=e.indexOf(r);i!==-1&&e.remove(i)}),this._attachedPortal=n,r}attachDomPortal=n=>{let e=n.element;e.parentNode;let r=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(r,e),this.outletElement.appendChild(e),this._attachedPortal=n,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(e,r)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(n){return n.hostView.rootNodes[0]}};var ig=(()=>{class t extends Xd{_moduleRef=u(Fn,{optional:!0});_document=u(G);_viewContainerRef=u(bt);_isInitialized=!1;_attachedRef=null;get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new C;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let r=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,i=r.createComponent(e.component,{index:r.length,injector:e.injector||r.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0,directives:e.directives||void 0});return r!==this._viewContainerRef&&this._getRootNode().appendChild(i.hostView.rootNodes[0]),super.setDisposeFn(()=>i.destroy()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachTemplatePortal(e){e.setAttachedHost(this);let r=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachDomPortal=e=>{let r=e.element;r.parentNode;let i=this._document.createComment("dom-portal");e.setAttachedHost(this),r.parentNode.insertBefore(i,r),this._getRootNode().appendChild(r),this._attachedPortal=e,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(r,i)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=(()=>{let e;return function(i){return(e||(e=gn(t)))(i||t)}})();static \u0275dir=j({type:t,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[nt]})}return t})(),eu=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({})}return t})();var HC=Vw();function su(t){return new tu(t.get(Go),t.get(G))}var tu=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(n,e){this._viewportRuler=n,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let n=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=n.style.left||"",this._previousHTMLStyles.top=n.style.top||"",n.style.left=Qe(-this._previousScrollPosition.left),n.style.top=Qe(-this._previousScrollPosition.top),n.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let n=this._document.documentElement,e=this._document.body,r=n.style,i=e.style,o=r.scrollBehavior||"",a=i.scrollBehavior||"";this._isEnabled=!1,r.left=this._previousHTMLStyles.left,r.top=this._previousHTMLStyles.top,n.classList.remove("cdk-global-scrollblock"),HC&&(r.scrollBehavior=i.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),HC&&(r.scrollBehavior=o,i.scrollBehavior=a)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,r=this._viewportRuler.getViewportSize();return e.scrollHeight>r.height||e.scrollWidth>r.width}};function KC(t,n){return new nu(t.get($o),t.get(T),t.get(Go),n)}var nu=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(n,e,r,i){this._scrollDispatcher=n,this._ngZone=e,this._viewportRuler=r,this._config=i}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(this._scrollSubscription)return;let n=this._scrollDispatcher.scrolled(0).pipe(Ye(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=n.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=n.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var Bs=class{enable(){}disable(){}attach(){}};function ag(t,n){return n.some(e=>{let r=t.bottom<e.top,i=t.top>e.bottom,o=t.right<e.left,a=t.left>e.right;return r||i||o||a})}function UC(t,n){return n.some(e=>{let r=t.top<e.top,i=t.bottom>e.bottom,o=t.left<e.left,a=t.right>e.right;return r||i||o||a})}function Yo(t,n){return new ru(t.get($o),t.get(Go),t.get(T),n)}var ru=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(n,e,r,i){this._scrollDispatcher=n,this._viewportRuler=e,this._ngZone=r,this._config=i}attach(n){this._overlayRef,this._overlayRef=n}enable(){if(!this._scrollSubscription){let n=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(n).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:r,height:i}=this._viewportRuler.getViewportSize();ag(e,[{width:r,height:i,bottom:i,right:r,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},ZC=(()=>{class t{_injector=u(J);noop=()=>new Bs;close=e=>KC(this._injector,e);block=()=>su(this._injector);reposition=e=>Yo(this._injector,e);static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),Wo=class{positionStrategy;scrollStrategy=new Bs;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(n){if(n){let e=Object.keys(n);for(let r of e)n[r]!==void 0&&(this[r]=n[r])}}};var iu=class{connectionPair;scrollableViewProperties;constructor(n,e){this.connectionPair=n,this.scrollableViewProperties=e}};var QC=(()=>{class t{_attachedOverlays=[];_document=u(G);_isAttached=!1;ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let r=this._attachedOverlays.indexOf(e);r>-1&&this._attachedOverlays.splice(r,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,r,i){return i.observers.length<1?!1:e.eventPredicate?e.eventPredicate(r):!0}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),XC=(()=>{class t extends QC{_ngZone=u(T);_renderer=u(Xe).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let r=this._attachedOverlays;for(let i=r.length-1;i>-1;i--){let o=r[i];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),JC=(()=>{class t extends QC{_platform=u(he);_ngZone=u(T);_renderer=u(Xe).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let r=this._document.body,i={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(r,"pointerdown",this._pointerDownListener,i),o.listen(r,"click",this._clickListener,i),o.listen(r,"auxclick",this._clickListener,i),o.listen(r,"contextmenu",this._clickListener,i)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=r.style.cursor,r.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=Nt(e)};_clickListener=e=>{let r=Nt(e),i=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:r;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let a=o.length-1;a>-1;a--){let s=o[a],l=s._outsidePointerEvents;if(!(!s.hasAttached()||!this.canReceiveEvent(s,e,l))){if($C(s.overlayElement,r)||$C(s.overlayElement,i))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();function $C(t,n){let e=typeof ShadowRoot<"u"&&ShadowRoot,r=n;for(;r;){if(r===t)return!0;r=e&&r instanceof ShadowRoot?r.host:r.parentNode}return!1}var eD=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(r,i){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2})}return t})(),tD=(()=>{class t{_platform=u(he);_containerElement;_document=u(G);_styleLoader=u(Me);ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||Th()){let i=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<i.length;o++)i[o].remove()}let r=this._document.createElement("div");r.classList.add(e),Th()?r.setAttribute("platform","test"):this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._containerElement=r}_loadStyles(){this._styleLoader.load(eD)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),sg=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(n,e,r,i){this._renderer=e,this._ngZone=r,this.element=n.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",i)}detach(){this._ngZone.runOutsideAngular(()=>{let n=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(n,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),n.style.pointerEvents="none",n.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function lg(t){return t&&t.nodeType===1}var og=A([]),ou=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new E;_attachments=new E;_detachments=new E;_positionStrategy;_scrollStrategy;_locationChanges;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new E;_outsidePointerEvents=new E;_afterNextRenderRef;constructor(n,e,r,i,o,a,s,l,c,d=!1,m,p){this._portalOutlet=n,this._host=e,this._pane=r,this._config=i,this._ngZone=o,this._keyboardDispatcher=a,this._document=s,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=d,this._injector=m,this._renderer=p,i.scrollStrategy&&(this._scrollStrategy=i.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=i.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(n){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(n);if(this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),Ie(()=>{og.update(r=>r.includes(this)?r:[...r,this])}),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=it(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation===!0||this._config.disposeOnNavigation==="pop-state"){let r=this._location.subscribe(()=>this.dispose());this._locationChanges=()=>r.unsubscribe()}else this._config.disposeOnNavigation==="url-change"&&(this._locationChanges=this._location.onUrlChange(()=>this.dispose()));return this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let n=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges?.(),this._outsideClickDispatcher.remove(this),Ie(()=>{og.update(e=>e.filter(r=>r!==this))}),n}dispose(){if(this._disposed)return;let n=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges?.(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,n&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0,Ie(()=>{og.update(e=>e.filter(r=>r!==this))})}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(n){n!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=n,this.hasAttached()&&(n.attach(this),this.updatePosition()))}updateSize(n){this._config=g(g({},this._config),n),this._updateElementSize()}setDirection(n){this._config=R(g({},this._config),{direction:n}),this._updateElementDirection()}addPanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!0)}removePanelClass(n){this._pane&&this._toggleClasses(this._pane,n,!1)}getDirection(){let n=this._config.direction;return n?typeof n=="string"?n:n.value:"ltr"}updateScrollStrategy(n){n!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=n,this.hasAttached()&&(n.attach(this),n.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let n=this._pane.style;n.width=Qe(this._config.width),n.height=Qe(this._config.height),n.minWidth=Qe(this._config.minWidth),n.minHeight=Qe(this._config.minHeight),n.maxWidth=Qe(this._config.maxWidth),n.maxHeight=Qe(this._config.maxHeight)}_togglePointerEvents(n){this._pane.style.pointerEvents=n?"":"none"}_attachHost(){if(!this._host.parentElement){let n=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;lg(n)?n.after(this._host):n?.type==="parent"?n.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch{}}_attachBackdrop(){let n="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new sg(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(n))}):this._backdropRef.element.classList.add(n)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(n,e,r){let i=Ao(e||[]).filter(o=>!!o);i.length&&(r?n.classList.add(...i):n.classList.remove(...i))}_detachContentWhenEmpty(){let n=!1;try{this._detachContentAfterRenderRef=it(()=>{n=!0,this._detachContent()},{injector:this._injector})}catch(e){if(n)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let n=this._scrollStrategy;n?.disable(),n?.detach?.()}},GC="cdk-overlay-connected-position-bounding-box",xR=/([A-Za-z%]+)$/;function Ko(t,n){return new qo(n,t.get(Go),t.get(G),t.get(he),t.get(tD))}var qo=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new E;_resizeSubscription=ue.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(n,e,r,i,o){this._viewportRuler=e,this._document=r,this._platform=i,this._overlayContainer=o,this.setOrigin(n)}attach(n){this._overlayRef&&this._overlayRef,this._validatePositions(),n.hostElement.classList.add(GC),this._overlayRef=n,this._boundingBox=n.hostElement,this._pane=n.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let n=this._originRect,e=this._overlayRect,r=this._viewportRect,i=this._containerRect,o=[],a;for(let s of this._preferredPositions){let l=this._getOriginPoint(n,i,s),c=this._getOverlayPoint(l,e,s),d=this._getOverlayFit(c,e,r,s);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(s,l);return}if(this._canFitWithFlexibleDimensions(d,c,r)){o.push({position:s,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,s)});continue}(!a||a.overlayFit.visibleArea<d.visibleArea)&&(a={overlayFit:d,overlayPoint:c,originPoint:l,position:s,overlayRect:e})}if(o.length){let s=null,l=-1;for(let c of o){let d=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);d>l&&(l=d,s=c)}this._isPushed=!1,this._applyPosition(s.position,s.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(a.position,a.originPoint);return}this._applyPosition(a.position,a.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Mi(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(GC),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let n=this._lastPosition;n?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(n,this._getOriginPoint(this._originRect,this._containerRect,n))):this.apply()}withScrollableContainers(n){return this._scrollables=n,this}withPositions(n){return this._preferredPositions=n,n.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(n){return this._viewportMargin=n,this}withFlexibleDimensions(n=!0){return this._hasFlexibleDimensions=n,this}withGrowAfterOpen(n=!0){return this._growAfterOpen=n,this}withPush(n=!0){return this._canPush=n,this}withLockedPosition(n=!0){return this._positionLocked=n,this}setOrigin(n){return this._origin=n,this}withDefaultOffsetX(n){return this._offsetX=n,this}withDefaultOffsetY(n){return this._offsetY=n,this}withTransformOriginOn(n){return this._transformOriginSelector=n,this}withPopoverLocation(n){return this._popoverLocation=n,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof V?this._origin.nativeElement:lg(this._origin)?this._origin:null}_getOriginPoint(n,e,r){let i;if(r.originX=="center")i=n.left+n.width/2;else{let a=this._isRtl()?n.right:n.left,s=this._isRtl()?n.left:n.right;i=r.originX=="start"?a:s}e.left<0&&(i-=e.left);let o;return r.originY=="center"?o=n.top+n.height/2:o=r.originY=="top"?n.top:n.bottom,e.top<0&&(o-=e.top),{x:i,y:o}}_getOverlayPoint(n,e,r){let i;r.overlayX=="center"?i=-e.width/2:r.overlayX==="start"?i=this._isRtl()?-e.width:0:i=this._isRtl()?0:-e.width;let o;return r.overlayY=="center"?o=-e.height/2:o=r.overlayY=="top"?0:-e.height,{x:n.x+i,y:n.y+o}}_getOverlayFit(n,e,r,i){let o=qC(e),{x:a,y:s}=n,l=this._getOffset(i,"x"),c=this._getOffset(i,"y");l&&(a+=l),c&&(s+=c);let d=0-a,m=a+o.width-r.width,p=0-s,f=s+o.height-r.height,h=this._subtractOverflows(o.width,d,m),w=this._subtractOverflows(o.height,p,f),I=h*w;return{visibleArea:I,isCompletelyWithinViewport:o.width*o.height===I,fitsInViewportVertically:w===o.height,fitsInViewportHorizontally:h==o.width}}_canFitWithFlexibleDimensions(n,e,r){if(this._hasFlexibleDimensions){let i=r.bottom-e.y,o=r.right-e.x,a=WC(this._overlayRef.getConfig().minHeight),s=WC(this._overlayRef.getConfig().minWidth),l=n.fitsInViewportVertically||a!=null&&a<=i,c=n.fitsInViewportHorizontally||s!=null&&s<=o;return l&&c}return!1}_pushOverlayOnScreen(n,e,r){if(this._previousPushAmount&&this._positionLocked)return{x:n.x+this._previousPushAmount.x,y:n.y+this._previousPushAmount.y};let i=qC(e),o=this._viewportRect,a=Math.max(n.x+i.width-o.width,0),s=Math.max(n.y+i.height-o.height,0),l=Math.max(o.top-r.top-n.y,0),c=Math.max(o.left-r.left-n.x,0),d=0,m=0;return i.width<=o.width?d=c||-a:d=n.x<this._getViewportMarginStart()?o.left-r.left-n.x:0,i.height<=o.height?m=l||-s:m=n.y<this._getViewportMarginTop()?o.top-r.top-n.y:0,this._previousPushAmount={x:d,y:m},{x:n.x+d,y:n.y+m}}_applyPosition(n,e){if(this._setTransformOrigin(n),this._setOverlayElementStyles(e,n),this._setBoundingBoxStyles(e,n),n.panelClass&&this._addPanelClasses(n.panelClass),this._positionChanges.observers.length){let r=this._getScrollVisibility();if(n!==this._lastPosition||!this._lastScrollVisibility||!wR(this._lastScrollVisibility,r)){let i=new iu(n,r);this._positionChanges.next(i)}this._lastScrollVisibility=r}this._lastPosition=n,this._isInitialRender=!1}_setTransformOrigin(n){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),r,i=n.overlayY;n.overlayX==="center"?r="center":this._isRtl()?r=n.overlayX==="start"?"right":"left":r=n.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${r} ${i}`}_calculateBoundingBoxRect(n,e){let r=this._viewportRect,i=this._isRtl(),o,a,s;if(e.overlayY==="top")a=n.y,o=r.height-a+this._getViewportMarginBottom();else if(e.overlayY==="bottom")s=r.height-n.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=r.height-s+this._getViewportMarginTop();else{let f=Math.min(r.bottom-n.y+r.top,n.y),h=this._lastBoundingBoxSize.height;o=f*2,a=n.y-f,o>h&&!this._isInitialRender&&!this._growAfterOpen&&(a=n.y-h/2)}let l=e.overlayX==="start"&&!i||e.overlayX==="end"&&i,c=e.overlayX==="end"&&!i||e.overlayX==="start"&&i,d,m,p;if(c)p=r.width-n.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=n.x-this._getViewportMarginStart();else if(l)m=n.x,d=r.right-n.x-this._getViewportMarginEnd();else{let f=Math.min(r.right-n.x+r.left,n.x),h=this._lastBoundingBoxSize.width;d=f*2,m=n.x-f,d>h&&!this._isInitialRender&&!this._growAfterOpen&&(m=n.x-h/2)}return{top:a,left:m,bottom:s,right:p,width:d,height:o}}_setBoundingBoxStyles(n,e){let r=this._calculateBoundingBoxRect(n,e);!this._isInitialRender&&!this._growAfterOpen&&(r.height=Math.min(r.height,this._lastBoundingBoxSize.height),r.width=Math.min(r.width,this._lastBoundingBoxSize.width));let i={};if(this._hasExactPosition())i.top=i.left="0",i.bottom=i.right="auto",i.maxHeight=i.maxWidth="",i.width=i.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,a=this._overlayRef.getConfig().maxWidth;i.width=Qe(r.width),i.height=Qe(r.height),i.top=Qe(r.top)||"auto",i.bottom=Qe(r.bottom)||"auto",i.left=Qe(r.left)||"auto",i.right=Qe(r.right)||"auto",e.overlayX==="center"?i.alignItems="center":i.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?i.justifyContent="center":i.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(i.maxHeight=Qe(o)),a&&(i.maxWidth=Qe(a))}this._lastBoundingBoxSize=r,Mi(this._boundingBox.style,i)}_resetBoundingBoxStyles(){Mi(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Mi(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(n,e){let r={},i=this._hasExactPosition(),o=this._hasFlexibleDimensions,a=this._overlayRef.getConfig();if(i){let d=this._viewportRuler.getViewportScrollPosition();Mi(r,this._getExactOverlayY(e,n,d)),Mi(r,this._getExactOverlayX(e,n,d))}else r.position="static";let s="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(s+=`translateX(${l}px) `),c&&(s+=`translateY(${c}px)`),r.transform=s.trim(),a.maxHeight&&(i?r.maxHeight=Qe(a.maxHeight):o&&(r.maxHeight="")),a.maxWidth&&(i?r.maxWidth=Qe(a.maxWidth):o&&(r.maxWidth="")),Mi(this._pane.style,r)}_getExactOverlayY(n,e,r){let i={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,n);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,r)),n.overlayY==="bottom"){let a=this._document.documentElement.clientHeight;i.bottom=`${a-(o.y+this._overlayRect.height)}px`}else i.top=Qe(o.y);return i}_getExactOverlayX(n,e,r){let i={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,n);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,r));let a;if(this._isRtl()?a=n.overlayX==="end"?"left":"right":a=n.overlayX==="end"?"right":"left",a==="right"){let s=this._document.documentElement.clientWidth;i.right=`${s-(o.x+this._overlayRect.width)}px`}else i.left=Qe(o.x);return i}_getScrollVisibility(){let n=this._getOriginRect(),e=this._pane.getBoundingClientRect(),r=this._scrollables.map(i=>i.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:UC(n,r),isOriginOutsideView:ag(n,r),isOverlayClipped:UC(e,r),isOverlayOutsideView:ag(e,r)}}_subtractOverflows(n,...e){return e.reduce((r,i)=>r-Math.max(i,0),n)}_getNarrowedViewportRect(){let n=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,r=this._viewportRuler.getViewportScrollPosition();return{top:r.top+this._getViewportMarginTop(),left:r.left+this._getViewportMarginStart(),right:r.left+n-this._getViewportMarginEnd(),bottom:r.top+e-this._getViewportMarginBottom(),width:n-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(n,e){return e==="x"?n.offsetX==null?this._offsetX:n.offsetX:n.offsetY==null?this._offsetY:n.offsetY}_validatePositions(){}_addPanelClasses(n){this._pane&&Ao(n).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(n=>{this._pane.classList.remove(n)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let n=this._origin;if(n instanceof V)return n.nativeElement.getBoundingClientRect();if(n instanceof Element)return n.getBoundingClientRect();let e=n.width||0,r=n.height||0;return{top:n.y,bottom:n.y+r,left:n.x,right:n.x+e,height:r,width:e}}_getContainerRect(){let n=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();n&&(e.style.display="block");let r=e.getBoundingClientRect();return n&&(e.style.display=""),r}};function Mi(t,n){for(let e in n)Object.hasOwn(n,e)&&(t[e]=n[e]);return t}function WC(t){if(typeof t!="number"&&t!=null){let[n,e]=t.split(xR);return!e||e==="px"?parseFloat(n):null}return t||null}function qC(t){return{top:Math.floor(t.top),right:Math.floor(t.right),bottom:Math.floor(t.bottom),left:Math.floor(t.left),width:Math.floor(t.width),height:Math.floor(t.height)}}function wR(t,n){return t===n?!0:t.isOriginClipped===n.isOriginClipped&&t.isOriginOutsideView===n.isOriginOutsideView&&t.isOverlayClipped===n.isOverlayClipped&&t.isOverlayOutsideView===n.isOverlayOutsideView}var YC="cdk-global-overlay-wrapper";function lu(t){return new au}var au=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(n){let e=n.getConfig();this._overlayRef=n,this._width&&!e.width&&n.updateSize({width:this._width}),this._height&&!e.height&&n.updateSize({height:this._height}),n.hostElement.classList.add(YC),this._isDisposed=!1}top(n=""){return this._bottomOffset="",this._topOffset=n,this._alignItems="flex-start",this}left(n=""){return this._xOffset=n,this._xPosition="left",this}bottom(n=""){return this._topOffset="",this._bottomOffset=n,this._alignItems="flex-end",this}right(n=""){return this._xOffset=n,this._xPosition="right",this}start(n=""){return this._xOffset=n,this._xPosition="start",this}end(n=""){return this._xOffset=n,this._xPosition="end",this}width(n=""){return this._overlayRef?this._overlayRef.updateSize({width:n}):this._width=n,this}height(n=""){return this._overlayRef?this._overlayRef.updateSize({height:n}):this._height=n,this}centerHorizontally(n=""){return this.left(n),this._xPosition="center",this}centerVertically(n=""){return this.top(n),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,r=this._overlayRef.getConfig(),{width:i,height:o,maxWidth:a,maxHeight:s}=r,l=(i==="100%"||i==="100vw")&&(!a||a==="100%"||a==="100vw"),c=(o==="100%"||o==="100vh")&&(!s||s==="100%"||s==="100vh"),d=this._xPosition,m=this._xOffset,p=this._overlayRef.getConfig().direction==="rtl",f="",h="",w="";l?w="flex-start":d==="center"?(w="center",p?h=m:f=m):p?d==="left"||d==="end"?(w="flex-end",f=m):(d==="right"||d==="start")&&(w="flex-start",h=m):d==="left"||d==="start"?(w="flex-start",f=m):(d==="right"||d==="end")&&(w="flex-end",h=m),n.position=this._cssPosition,n.marginLeft=l?"0":f,n.marginTop=c?"0":this._topOffset,n.marginBottom=this._bottomOffset,n.marginRight=l?"0":h,e.justifyContent=w,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let n=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,r=e.style;e.classList.remove(YC),r.justifyContent=r.alignItems=n.marginTop=n.marginBottom=n.marginLeft=n.marginRight=n.position="",this._overlayRef=null,this._isDisposed=!0}},nD=(()=>{class t{_injector=u(J);global(){return lu()}flexibleConnectedTo(e){return Ko(this._injector,e)}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),rD=new b("OVERLAY_DEFAULT_CONFIG");function Zo(t,n){t.get(Me).load(eD);let e=t.get(tD),r=t.get(G),i=t.get(je),o=t.get(Kt),a=t.get(Tt),s=t.get(Te,null,{optional:!0})||t.get(Xe).createRenderer(null,null),l=new Wo(n),c=t.get(rD,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||a.value,!r.body||!("showPopover"in r.body)?l.usePopover=!1:l.usePopover=n?.usePopover??c;let d=r.createElement("div"),m=r.createElement("div");d.id=i.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),m.appendChild(d),l.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let p=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return lg(p)?p.after(m):p?.type==="parent"?p.element.appendChild(m):e.getContainerElement().appendChild(m),new ou(new Jd(d,o,t),m,d,l,t.get(T),t.get(XC),r,t.get(Dr),t.get(JC),n?.disableAnimations??t.get(Ra,null,{optional:!0})==="NoopAnimations",t.get(Le),s)}var iD=(()=>{class t{scrollStrategies=u(ZC);_positionBuilder=u(nD);_injector=u(J);create(e){return Zo(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})();var cg=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({providers:[iD],imports:[Fe,eu,ng,ng]})}return t})();var CR=20;var DR=new b("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(J);return()=>Yo(t,{scrollThrottle:CR})}}),SR=new b("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var oD="tooltip-panel",ER={passive:!0},kR=8,IR=8,MR=24,NR=200,aD=(()=>{class t{_elementRef=u(V);_ngZone=u(T);_platform=u(he);_ariaDescriber=u(Lw);_focusMonitor=u(en);_dir=u(Tt);_injector=u(J);_viewContainerRef=u(bt);_mediaMatcher=u(Ro);_document=u(G);_renderer=u(Te);_animationsDisabled=Be();_defaultOptions=u(SR,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=TR;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=tr(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let r=tr(e);this._disabled!==r&&(this._disabled=r,r?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=To(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=To(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let r=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(r)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new E;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=kR}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(Ue(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(r=>r()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,r){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let i=this._createOverlay(r);this._detach(),this._portal=this._portal||new Rr(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=i.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(Ue(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let r=this._tooltipInstance;r&&(r.isVisible()?r.hide(e):(r._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let a=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&a._origin instanceof V)return this._overlayRef;this._detach()}let r=this._injector.get($o).getAncestorScrollContainers(this._elementRef),i=`${this._cssClassPrefix}-${oD}`,o=Ko(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(r).withPopoverLocation("global");return o.positionChanges.pipe(Ue(this._destroyed)).subscribe(a=>{this._updateCurrentPositionClass(a.connectionPair),this._tooltipInstance&&a.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=Zo(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,i]:i,scrollStrategy:this._injector.get(DR)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(Ue(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(Ue(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(Ue(this._destroyed)).subscribe(a=>{a.preventDefault(),a.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(Ue(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let r=e.getConfig().positionStrategy,i=this._getOrigin(),o=this._getOverlayPosition();r.withPositions([this._addOffset(g(g({},i.main),o.main)),this._addOffset(g(g({},i.fallback),o.fallback))])}_addOffset(e){let r=IR,i=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-r:e.originY==="bottom"?e.offsetY=r:e.originX==="start"?e.offsetX=i?-r:r:e.originX==="end"&&(e.offsetX=i?r:-r),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",r=this.position,i;r=="above"||r=="below"?i={originX:"center",originY:r=="above"?"top":"bottom"}:r=="before"||r=="left"&&e||r=="right"&&!e?i={originX:"start",originY:"center"}:(r=="after"||r=="right"&&e||r=="left"&&!e)&&(i={originX:"end",originY:"center"});let{x:o,y:a}=this._invertPosition(i.originX,i.originY);return{main:i,fallback:{originX:o,originY:a}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",r=this.position,i;r=="above"?i={overlayX:"center",overlayY:"bottom"}:r=="below"?i={overlayX:"center",overlayY:"top"}:r=="before"||r=="left"&&e||r=="right"&&!e?i={overlayX:"end",overlayY:"center"}:(r=="after"||r=="right"&&e||r=="left"&&!e)&&(i={overlayX:"start",overlayY:"center"});let{x:o,y:a}=this._invertPosition(i.overlayX,i.overlayY);return{main:i,fallback:{overlayX:o,overlayY:a}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),it(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,r){return this.position==="above"||this.position==="below"?r==="top"?r="bottom":r==="bottom"&&(r="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:r}}_updateCurrentPositionClass(e){let{overlayY:r,originX:i,originY:o}=e,a;if(r==="center"?this._dir&&this._dir.value==="rtl"?a=i==="end"?"left":"right":a=i==="start"?"left":"right":a=r==="bottom"&&o==="top"?"above":"below",a!==this._currentPosition){let s=this._overlayRef;if(s){let l=`${this._cssClassPrefix}-${oD}-`;s.removePanelClass(l+this._currentPosition),s.addPanelClass(l+a)}this._currentPosition=a}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let r=e.targetTouches?.[0],i=r?{x:r.clientX,y:r.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,i)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let r;e.x!==void 0&&e.y!==void 0&&(r=e),this.show(void 0,r)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let r=e.relatedTarget;(!r||!this._overlayRef?.overlayElement.contains(r))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let r=this._document.elementFromPoint(e.clientX,e.clientY),i=this._elementRef.nativeElement;r!==i&&!i.contains(r)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,r){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,r,ER))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let r=this._elementRef.nativeElement,i=r.style;(e==="on"||r.nodeName!=="INPUT"&&r.nodeName!=="TEXTAREA")&&(i.userSelect=i.msUserSelect=i.webkitUserSelect=i.MozUserSelect="none"),(e==="on"||!r.draggable)&&(i.webkitUserDrag="none"),i.touchAction="none",i.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||it({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!xn(e):!0;static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(r,i){r&2&&U("mat-mdc-tooltip-disabled",i.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return t})(),TR=(()=>{class t{_changeDetectorRef=u(ve);_elementRef=u(V);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Be();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new E;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>MR&&e.width>=NR}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let r=this._tooltip.nativeElement,i=this._showAnimation,o=this._hideAnimation;if(r.classList.remove(e?o:i),r.classList.add(e?i:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let a=getComputedStyle(r);(a.getPropertyValue("animation-duration")==="0s"||a.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(r.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["tooltip"];return k({type:t,selectors:[["mat-tooltip-component"]],viewQuery:function(i,o){if(i&1&&He(e,7),i&2){let a;ae(a=se())&&(o._tooltip=a.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,o){i&1&&B("mouseleave",function(s){return o._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,o){i&1&&(Ne(0,"div",1,0),lo("animationend",function(s){return o._handleAnimationEnd(s)}),Ne(2,"div",2),M(3),Re()()),i&2&&(Vt(o.tooltipClass),U("mdc-tooltip--multiline",o._isMultiline),_(3),Qt(o.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--%NS%mat-tooltip-container-color, var(--%NS%mat-sys-inverse-surface));
  color: var(--%NS%mat-tooltip-supporting-text-color, var(--%NS%mat-sys-inverse-on-surface));
  border-radius: var(--%NS%mat-tooltip-container-shape, var(--%NS%mat-sys-corner-extra-small));
  font-family: var(--%NS%mat-tooltip-supporting-text-font, var(--%NS%mat-sys-body-small-font));
  font-size: var(--%NS%mat-tooltip-supporting-text-size, var(--%NS%mat-sys-body-small-size));
  font-weight: var(--%NS%mat-tooltip-supporting-text-weight, var(--%NS%mat-sys-body-small-weight));
  line-height: var(--%NS%mat-tooltip-supporting-text-line-height, var(--%NS%mat-sys-body-small-line-height));
  letter-spacing: var(--%NS%mat-tooltip-supporting-text-tracking, var(--%NS%mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2})})()}return t})();var ta=(()=>{class t{changes=new E;calendarLabel="Calendar";openCalendarLabel="Open calendar";closeCalendarLabel="Close calendar";prevMonthLabel="Previous month";nextMonthLabel="Next month";prevYearLabel="Previous year";nextYearLabel="Next year";prevMultiYearLabel="Previous 24 years";nextMultiYearLabel="Next 24 years";switchToMonthViewLabel="Choose date";switchToMultiYearViewLabel="Choose month and year";startDateLabel="Start date";endDateLabel="End date";comparisonDateLabel="Comparison range";formatYearRange(e,r){return`${e} \u2013 ${r}`}formatYearRangeLabel(e,r){return`${e} to ${r}`}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac})}return t})(),AR=0,Hs=class{value;displayValue;ariaLabel;enabled;compareValue;rawValue;id=AR++;cssClasses;constructor(n,e,r,i,o,a=n,s){this.value=n,this.displayValue=e,this.ariaLabel=r,this.enabled=i,this.compareValue=a,this.rawValue=s,this.cssClasses=o instanceof Set?Array.from(o):o}},RR={passive:!1,capture:!0},du={passive:!0,capture:!0},sD={passive:!0},Xo=(()=>{class t{_elementRef=u(V);_ngZone=u(T);_platform=u(he);_intl=u(ta);_eventCleanups;_skipNextFocus=!1;_focusActiveCellAfterViewChecked=!1;label;rows;todayValue;startValue;endValue;labelMinRequiredCells;numCols=7;activeCell=0;ngAfterViewChecked(){this._focusActiveCellAfterViewChecked&&(this._focusActiveCell(),this._focusActiveCellAfterViewChecked=!1)}isRange=!1;cellAspectRatio=1;comparisonStart=null;comparisonEnd=null;previewStart=null;previewEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;selectedValueChange=new C;previewChange=new C;activeDateChange=new C;dragStarted=new C;dragEnded=new C;_firstRowOffset;_cellPadding;_cellWidth;_startDateLabelId;_endDateLabelId;_comparisonStartDateLabelId;_comparisonEndDateLabelId;_didDragSinceMouseDown=!1;_injector=u(J);comparisonDateAccessibleName=this._intl.comparisonDateLabel;_trackRow=e=>e;constructor(){let e=u(Te),r=u(je);this._startDateLabelId=r.getId("mat-calendar-body-start-"),this._endDateLabelId=r.getId("mat-calendar-body-end-"),this._comparisonStartDateLabelId=r.getId("mat-calendar-body-comparison-start-"),this._comparisonEndDateLabelId=r.getId("mat-calendar-body-comparison-end-"),u(Me).load(nn),this._ngZone.runOutsideAngular(()=>{let i=this._elementRef.nativeElement,o=[e.listen(i,"touchmove",this._touchmoveHandler,RR),e.listen(i,"mouseenter",this._enterHandler,du),e.listen(i,"focus",this._enterHandler,du),e.listen(i,"mouseleave",this._leaveHandler,du),e.listen(i,"blur",this._leaveHandler,du),e.listen(i,"mousedown",this._mousedownHandler,sD),e.listen(i,"touchstart",this._mousedownHandler,sD)];this._platform.isBrowser&&o.push(e.listen("window","mouseup",this._mouseupHandler),e.listen("window","touchend",this._touchendHandler)),this._eventCleanups=o})}_cellClicked(e,r){this._didDragSinceMouseDown||e.enabled&&this.selectedValueChange.emit({value:e.value,event:r})}_emitActiveDateChange(e,r){e.enabled&&this.activeDateChange.emit({value:e.value,event:r})}_isSelected(e){return this.startValue===e||this.endValue===e}ngOnChanges(e){let r=e.numCols,{rows:i,numCols:o}=this;(e.rows||r)&&(this._firstRowOffset=i&&i.length&&i[0].length?o-i[0].length:0),(e.cellAspectRatio||r||!this._cellPadding)&&(this._cellPadding=`${50*this.cellAspectRatio/o}%`),(r||!this._cellWidth)&&(this._cellWidth=`${100/o}%`)}ngOnDestroy(){this._eventCleanups.forEach(e=>e())}_isActiveCell(e,r){let i=e*this.numCols+r;return e&&(i-=this._firstRowOffset),i==this.activeCell}_focusActiveCell(e=!0){it(()=>{setTimeout(()=>{let r=this._elementRef.nativeElement.querySelector(".mat-calendar-body-active");r&&(e||(this._skipNextFocus=!0),r.focus())})},{injector:this._injector})}_scheduleFocusActiveCellAfterViewChecked(){this._focusActiveCellAfterViewChecked=!0}_isRangeStart(e){return mg(e,this.startValue,this.endValue)}_isRangeEnd(e){return fg(e,this.startValue,this.endValue)}_isInRange(e){return pg(e,this.startValue,this.endValue,this.isRange)}_isComparisonStart(e){return mg(e,this.comparisonStart,this.comparisonEnd)}_isComparisonBridgeStart(e,r,i){if(!this._isComparisonStart(e)||this._isRangeStart(e)||!this._isInRange(e))return!1;let o=this.rows[r][i-1];if(!o){let a=this.rows[r-1];o=a&&a[a.length-1]}return o&&!this._isRangeEnd(o.compareValue)}_isComparisonBridgeEnd(e,r,i){if(!this._isComparisonEnd(e)||this._isRangeEnd(e)||!this._isInRange(e))return!1;let o=this.rows[r][i+1];if(!o){let a=this.rows[r+1];o=a&&a[0]}return o&&!this._isRangeStart(o.compareValue)}_isComparisonEnd(e){return fg(e,this.comparisonStart,this.comparisonEnd)}_isInComparisonRange(e){return pg(e,this.comparisonStart,this.comparisonEnd,this.isRange)}_isComparisonIdentical(e){return this.comparisonStart===this.comparisonEnd&&e===this.comparisonStart}_isPreviewStart(e){return mg(e,this.previewStart,this.previewEnd)}_isPreviewEnd(e){return fg(e,this.previewStart,this.previewEnd)}_isInPreview(e){return pg(e,this.previewStart,this.previewEnd,this.isRange)}_getDescribedby(e){if(!this.isRange)return null;if(this.startValue===e&&this.endValue===e)return`${this._startDateLabelId} ${this._endDateLabelId}`;if(this.startValue===e)return this._startDateLabelId;if(this.endValue===e)return this._endDateLabelId;if(this.comparisonStart!==null&&this.comparisonEnd!==null){if(e===this.comparisonStart&&e===this.comparisonEnd)return`${this._comparisonStartDateLabelId} ${this._comparisonEndDateLabelId}`;if(e===this.comparisonStart)return this._comparisonStartDateLabelId;if(e===this.comparisonEnd)return this._comparisonEndDateLabelId}return null}_enterHandler=e=>{if(this._skipNextFocus&&e.type==="focus"){this._skipNextFocus=!1;return}if(e.target&&this.isRange){let r=this._getCellFromElement(e.target);r&&this._ngZone.run(()=>this.previewChange.emit({value:r.enabled?r:null,event:e}))}};_touchmoveHandler=e=>{if(!this.isRange)return;let r=lD(e),i=r?this._getCellFromElement(r):null;r!==e.target&&(this._didDragSinceMouseDown=!0),ug(e.target)&&e.preventDefault(),this._ngZone.run(()=>this.previewChange.emit({value:i?.enabled?i:null,event:e}))};_leaveHandler=e=>{this.previewEnd!==null&&this.isRange&&(e.type!=="blur"&&(this._didDragSinceMouseDown=!0),e.target&&this._getCellFromElement(e.target)&&!(e.relatedTarget&&this._getCellFromElement(e.relatedTarget))&&this._ngZone.run(()=>this.previewChange.emit({value:null,event:e})))};_mousedownHandler=e=>{if(!this.isRange)return;this._didDragSinceMouseDown=!1;let r=e.target&&this._getCellFromElement(e.target);!r||!this._isInRange(r.compareValue)||this._ngZone.run(()=>{this.dragStarted.emit({value:r.rawValue,event:e})})};_mouseupHandler=e=>{if(!this.isRange)return;let r=ug(e.target);if(!r){this._ngZone.run(()=>{this.dragEnded.emit({value:null,event:e})});return}r.closest(".mat-calendar-body")===this._elementRef.nativeElement&&this._ngZone.run(()=>{let i=this._getCellFromElement(r);this.dragEnded.emit({value:i?.rawValue??null,event:e})})};_touchendHandler=e=>{let r=lD(e);r&&this._mouseupHandler({target:r})};_getCellFromElement(e){let r=ug(e);if(r){let i=r.getAttribute("data-mat-row"),o=r.getAttribute("data-mat-col");if(i&&o)return this.rows[parseInt(i)]?.[parseInt(o)]||null}return null}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){function e(l,c){return this._trackRow(c)}let r=(l,c)=>c.id;function i(l,c){if(l&1&&(Ne(0,"tr",0)(1,"td",3),M(2),Re()()),l&2){let d=fe();_(),bi("padding-top",d._cellPadding)("padding-bottom",d._cellPadding),de("colspan",d.numCols),_(),St(" ",d.label," ")}}function o(l,c){if(l&1&&(Ne(0,"td",3),M(1),Re()),l&2){let d=fe(2);bi("padding-top",d._cellPadding)("padding-bottom",d._cellPadding),de("colspan",d._firstRowOffset),_(),St(" ",d._firstRowOffset>=d.labelMinRequiredCells?d.label:""," ")}}function a(l,c){if(l&1){let d=xr();Ne(0,"td",6)(1,"button",7),lo("click",function(p){let f=dt(d).$implicit,h=fe(2);return ut(h._cellClicked(f,p))})("focus",function(p){let f=dt(d).$implicit,h=fe(2);return ut(h._emitActiveDateChange(f,p))}),Ne(2,"span",8),M(3),Re(),Mt(4,"span",9),Re()()}if(l&2){let d=c.$implicit,m=c.$index,p=fe().$index,f=fe();bi("width",f._cellWidth)("padding-top",f._cellPadding)("padding-bottom",f._cellPadding),de("data-mat-row",p)("data-mat-col",m),_(),Vt(d.cssClasses),U("mat-calendar-body-disabled",!d.enabled)("mat-calendar-body-active",f._isActiveCell(p,m))("mat-calendar-body-range-start",f._isRangeStart(d.compareValue))("mat-calendar-body-range-end",f._isRangeEnd(d.compareValue))("mat-calendar-body-in-range",f._isInRange(d.compareValue))("mat-calendar-body-comparison-bridge-start",f._isComparisonBridgeStart(d.compareValue,p,m))("mat-calendar-body-comparison-bridge-end",f._isComparisonBridgeEnd(d.compareValue,p,m))("mat-calendar-body-comparison-start",f._isComparisonStart(d.compareValue))("mat-calendar-body-comparison-end",f._isComparisonEnd(d.compareValue))("mat-calendar-body-in-comparison-range",f._isInComparisonRange(d.compareValue))("mat-calendar-body-preview-start",f._isPreviewStart(d.compareValue))("mat-calendar-body-preview-end",f._isPreviewEnd(d.compareValue))("mat-calendar-body-in-preview",f._isInPreview(d.compareValue)),mt("tabIndex",f._isActiveCell(p,m)?0:-1),de("aria-label",d.ariaLabel)("aria-disabled",!d.enabled||null)("aria-pressed",f._isSelected(d.compareValue))("aria-current",f.todayValue===d.compareValue?"date":null)("aria-describedby",f._getDescribedby(d.compareValue)),_(),U("mat-calendar-body-selected",f._isSelected(d.compareValue))("mat-calendar-body-comparison-identical",f._isComparisonIdentical(d.compareValue))("mat-calendar-body-today",f.todayValue===d.compareValue),_(),St(" ",d.displayValue," ")}}function s(l,c){if(l&1&&(Ne(0,"tr",1),xe(1,o,2,6,"td",4),pi(2,a,5,49,"td",5,r),Re()),l&2){let d=c.$implicit,m=c.$index,p=fe();_(),we(m===0&&p._firstRowOffset?1:-1),_(),hi(d)}}return k({type:t,selectors:[["","mat-calendar-body",""]],hostAttrs:[1,"mat-calendar-body"],inputs:{label:"label",rows:"rows",todayValue:"todayValue",startValue:"startValue",endValue:"endValue",labelMinRequiredCells:"labelMinRequiredCells",numCols:"numCols",activeCell:"activeCell",isRange:"isRange",cellAspectRatio:"cellAspectRatio",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd",previewStart:"previewStart",previewEnd:"previewEnd",startDateAccessibleName:"startDateAccessibleName",endDateAccessibleName:"endDateAccessibleName"},outputs:{selectedValueChange:"selectedValueChange",previewChange:"previewChange",activeDateChange:"activeDateChange",dragStarted:"dragStarted",dragEnded:"dragEnded"},exportAs:["matCalendarBody"],features:[Ae],decls:11,vars:11,consts:[["aria-hidden","true"],["role","row"],[1,"mat-calendar-body-hidden-label",3,"id"],[1,"mat-calendar-body-label"],[1,"mat-calendar-body-label",3,"paddingTop","paddingBottom"],["role","gridcell",1,"mat-calendar-body-cell-container",3,"width","paddingTop","paddingBottom"],["role","gridcell",1,"mat-calendar-body-cell-container"],["type","button",1,"mat-calendar-body-cell",3,"click","focus","tabindex"],[1,"mat-calendar-body-cell-content","mat-focus-indicator"],["aria-hidden","true",1,"mat-calendar-body-cell-preview"]],template:function(c,d){c&1&&(xe(0,i,3,6,"tr",0),pi(1,s,4,1,"tr",1,e,!0),Ne(3,"span",2),M(4),Re(),Ne(5,"span",2),M(6),Re(),Ne(7,"span",2),M(8),Re(),Ne(9,"span",2),M(10),Re()),c&2&&(we(d._firstRowOffset<d.labelMinRequiredCells?0:-1),_(),hi(d.rows),_(2),mt("id",d._startDateLabelId),_(),St(" ",d.startDateAccessibleName,`
`),_(),mt("id",d._endDateLabelId),_(),St(" ",d.endDateAccessibleName,`
`),_(),mt("id",d._comparisonStartDateLabelId),_(),vi(" ",d.comparisonDateAccessibleName," ",d.startDateAccessibleName,`
`),_(),mt("id",d._comparisonEndDateLabelId),_(),vi(" ",d.comparisonDateAccessibleName," ",d.endDateAccessibleName,`
`))},styles:[`.mat-calendar-body {
  min-width: 224px;
}

.mat-calendar-body-today:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  border-color: var(--%NS%mat-datepicker-calendar-date-today-outline-color, var(--%NS%mat-sys-primary));
}

.mat-calendar-body-label {
  height: 0;
  line-height: 0;
  text-align: start;
  padding-left: 4.7142857143%;
  padding-right: 4.7142857143%;
  font-size: var(--%NS%mat-datepicker-calendar-body-label-text-size, var(--%NS%mat-sys-title-small-size));
  font-weight: var(--%NS%mat-datepicker-calendar-body-label-text-weight, var(--%NS%mat-sys-title-small-weight));
  color: var(--%NS%mat-datepicker-calendar-body-label-text-color, var(--%NS%mat-sys-on-surface));
}

.mat-calendar-body-hidden-label {
  display: none;
}

.mat-calendar-body-cell-container {
  position: relative;
  height: 0;
  line-height: 0;
}

.mat-calendar-body-cell {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: none;
  text-align: center;
  outline: none;
  margin: 0;
  font-family: var(--%NS%mat-datepicker-calendar-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-datepicker-calendar-text-size, var(--%NS%mat-sys-body-medium-size));
  -webkit-user-select: none;
  user-select: none;
  cursor: pointer;
  outline: none;
  border: none;
  -webkit-tap-highlight-color: transparent;
}
.mat-calendar-body-cell::-moz-focus-inner {
  border: 0;
}

.mat-calendar-body-cell::before,
.mat-calendar-body-cell::after,
.mat-calendar-body-cell-preview {
  content: "";
  position: absolute;
  top: 5%;
  left: 0;
  z-index: 0;
  box-sizing: border-box;
  display: block;
  height: 90%;
  width: 100%;
}

.mat-calendar-body-range-start:not(.mat-calendar-body-in-comparison-range)::before,
.mat-calendar-body-range-start::after,
.mat-calendar-body-comparison-start:not(.mat-calendar-body-comparison-bridge-start)::before,
.mat-calendar-body-comparison-start::after,
.mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  left: 5%;
  width: 95%;
  border-top-left-radius: 999px;
  border-bottom-left-radius: 999px;
}
[dir=rtl] .mat-calendar-body-range-start:not(.mat-calendar-body-in-comparison-range)::before,
[dir=rtl] .mat-calendar-body-range-start::after,
[dir=rtl] .mat-calendar-body-comparison-start:not(.mat-calendar-body-comparison-bridge-start)::before,
[dir=rtl] .mat-calendar-body-comparison-start::after,
[dir=rtl] .mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  left: 0;
  border-radius: 0;
  border-top-right-radius: 999px;
  border-bottom-right-radius: 999px;
}

.mat-calendar-body-range-end:not(.mat-calendar-body-in-comparison-range)::before,
.mat-calendar-body-range-end::after,
.mat-calendar-body-comparison-end:not(.mat-calendar-body-comparison-bridge-end)::before,
.mat-calendar-body-comparison-end::after,
.mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  width: 95%;
  border-top-right-radius: 999px;
  border-bottom-right-radius: 999px;
}
[dir=rtl] .mat-calendar-body-range-end:not(.mat-calendar-body-in-comparison-range)::before,
[dir=rtl] .mat-calendar-body-range-end::after,
[dir=rtl] .mat-calendar-body-comparison-end:not(.mat-calendar-body-comparison-bridge-end)::before,
[dir=rtl] .mat-calendar-body-comparison-end::after,
[dir=rtl] .mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  left: 5%;
  border-radius: 0;
  border-top-left-radius: 999px;
  border-bottom-left-radius: 999px;
}

[dir=rtl] .mat-calendar-body-comparison-bridge-start.mat-calendar-body-range-end::after,
[dir=rtl] .mat-calendar-body-comparison-bridge-end.mat-calendar-body-range-start::after {
  width: 95%;
  border-top-right-radius: 999px;
  border-bottom-right-radius: 999px;
}

.mat-calendar-body-comparison-start.mat-calendar-body-range-end::after, [dir=rtl] .mat-calendar-body-comparison-start.mat-calendar-body-range-end::after,
.mat-calendar-body-comparison-end.mat-calendar-body-range-start::after,
[dir=rtl] .mat-calendar-body-comparison-end.mat-calendar-body-range-start::after {
  width: 90%;
}

.mat-calendar-body-in-preview {
  color: var(--%NS%mat-datepicker-calendar-date-preview-state-outline-color, var(--%NS%mat-sys-primary));
}
.mat-calendar-body-in-preview .mat-calendar-body-cell-preview {
  border-top: dashed 1px;
  border-bottom: dashed 1px;
}

.mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  border-left: dashed 1px;
}
[dir=rtl] .mat-calendar-body-preview-start .mat-calendar-body-cell-preview {
  border-left: 0;
  border-right: dashed 1px;
}

.mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  border-right: dashed 1px;
}
[dir=rtl] .mat-calendar-body-preview-end .mat-calendar-body-cell-preview {
  border-right: 0;
  border-left: dashed 1px;
}

.mat-calendar-body-disabled {
  cursor: default;
}
.mat-calendar-body-disabled > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  color: var(--%NS%mat-datepicker-calendar-date-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-calendar-body-disabled > .mat-calendar-body-today:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  border-color: var(--%NS%mat-datepicker-calendar-date-today-disabled-state-outline-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-calendar-body-disabled {
    opacity: 0.5;
  }
}

.mat-calendar-body-cell-content {
  top: 5%;
  left: 5%;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 90%;
  height: 90%;
  line-height: 1;
  border-width: 1px;
  border-style: solid;
  border-radius: 999px;
  color: var(--%NS%mat-datepicker-calendar-date-text-color, var(--%NS%mat-sys-on-surface));
  border-color: var(--%NS%mat-datepicker-calendar-date-outline-color, transparent);
}
.mat-calendar-body-cell-content.mat-focus-indicator {
  position: absolute;
}
.mat-calendar-body-cell-content::before {
  border-radius: 50%;
}
@media (forced-colors: active) {
  .mat-calendar-body-cell-content {
    border: none;
  }
}

.cdk-keyboard-focused .mat-calendar-body-active > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical), .cdk-program-focused .mat-calendar-body-active > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
  background-color: var(--%NS%mat-datepicker-calendar-date-focus-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-focus-state-layer-opacity) * 100%), transparent));
}

@media (hover: hover) {
  .mat-calendar-body-cell:not(.mat-calendar-body-disabled):hover > .mat-calendar-body-cell-content:not(.mat-calendar-body-selected):not(.mat-calendar-body-comparison-identical) {
    background-color: var(--%NS%mat-datepicker-calendar-date-hover-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) calc(var(--%NS%mat-sys-hover-state-layer-opacity) * 100%), transparent));
  }
}
.mat-calendar-body-selected {
  background-color: var(--%NS%mat-datepicker-calendar-date-selected-state-background-color, var(--%NS%mat-sys-primary));
  color: var(--%NS%mat-datepicker-calendar-date-selected-state-text-color, var(--%NS%mat-sys-on-primary));
}
.mat-calendar-body-disabled > .mat-calendar-body-selected {
  background-color: var(--%NS%mat-datepicker-calendar-date-selected-disabled-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-calendar-body-selected.mat-calendar-body-today {
  box-shadow: inset 0 0 0 1px var(--%NS%mat-datepicker-calendar-date-today-selected-state-outline-color, var(--%NS%mat-sys-primary));
}

.mat-calendar-body-in-range::before {
  background: var(--%NS%mat-datepicker-calendar-date-in-range-state-background-color, var(--%NS%mat-sys-primary-container));
}

.mat-calendar-body-comparison-identical,
.mat-calendar-body-in-comparison-range::before {
  background: var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container));
}

.mat-calendar-body-comparison-identical,
.mat-calendar-body-in-comparison-range::before {
  background: var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container));
}

.mat-calendar-body-comparison-bridge-start::before,
[dir=rtl] .mat-calendar-body-comparison-bridge-end::before {
  background: linear-gradient(to right, var(--%NS%mat-datepicker-calendar-date-in-range-state-background-color, var(--%NS%mat-sys-primary-container)) 50%, var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container)) 50%);
}

.mat-calendar-body-comparison-bridge-end::before,
[dir=rtl] .mat-calendar-body-comparison-bridge-start::before {
  background: linear-gradient(to left, var(--%NS%mat-datepicker-calendar-date-in-range-state-background-color, var(--%NS%mat-sys-primary-container)) 50%, var(--%NS%mat-datepicker-calendar-date-in-comparison-range-state-background-color, var(--%NS%mat-sys-tertiary-container)) 50%);
}

.mat-calendar-body-in-range > .mat-calendar-body-comparison-identical,
.mat-calendar-body-in-comparison-range.mat-calendar-body-in-range::after {
  background: var(--%NS%mat-datepicker-calendar-date-in-overlap-range-state-background-color, var(--%NS%mat-sys-secondary-container));
}

.mat-calendar-body-comparison-identical.mat-calendar-body-selected,
.mat-calendar-body-in-comparison-range > .mat-calendar-body-selected {
  background: var(--%NS%mat-datepicker-calendar-date-in-overlap-range-selected-state-background-color, var(--%NS%mat-sys-secondary));
}

@media (forced-colors: active) {
  .mat-datepicker-popup:not(:empty),
  .mat-calendar-body-cell:not(.mat-calendar-body-in-range) .mat-calendar-body-selected {
    outline: solid 1px;
  }
  .mat-calendar-body-today {
    outline: dotted 1px;
  }
  .mat-calendar-body-cell::before,
  .mat-calendar-body-cell::after,
  .mat-calendar-body-selected {
    background: none;
  }
  .mat-calendar-body-in-range::before,
  .mat-calendar-body-comparison-bridge-start::before,
  .mat-calendar-body-comparison-bridge-end::before {
    border-top: solid 1px;
    border-bottom: solid 1px;
  }
  .mat-calendar-body-range-start::before {
    border-left: solid 1px;
  }
  [dir=rtl] .mat-calendar-body-range-start::before {
    border-left: 0;
    border-right: solid 1px;
  }
  .mat-calendar-body-range-end::before {
    border-right: solid 1px;
  }
  [dir=rtl] .mat-calendar-body-range-end::before {
    border-right: 0;
    border-left: solid 1px;
  }
  .mat-calendar-body-in-comparison-range::before {
    border-top: dashed 1px;
    border-bottom: dashed 1px;
  }
  .mat-calendar-body-comparison-start::before {
    border-left: dashed 1px;
  }
  [dir=rtl] .mat-calendar-body-comparison-start::before {
    border-left: 0;
    border-right: dashed 1px;
  }
  .mat-calendar-body-comparison-end::before {
    border-right: dashed 1px;
  }
  [dir=rtl] .mat-calendar-body-comparison-end::before {
    border-right: 0;
    border-left: dashed 1px;
  }
}
`],encapsulation:2})})()}return t})();function dg(t){return t?.nodeName==="TD"}function ug(t){let n;return dg(t)?n=t:dg(t.parentNode)?n=t.parentNode:dg(t.parentNode?.parentNode)&&(n=t.parentNode.parentNode),n?.getAttribute("data-mat-row")!=null?n:null}function mg(t,n,e){return e!==null&&n!==e&&t<e&&t===n}function fg(t,n,e){return n!==null&&n!==e&&t>=n&&t===e}function pg(t,n,e,r){return r&&n!==null&&e!==null&&n!==e&&t>=n&&t<=e}function lD(t){let n=t.changedTouches[0];return document.elementFromPoint(n.clientX,n.clientY)}var sn=class{start;end;_disableStructuralEquivalency;constructor(n,e){this.start=n,this.end=e}},Us=(()=>{class t{selection;_adapter;_selectionChanged=new E;selectionChanged=this._selectionChanged;constructor(e,r){this.selection=e,this._adapter=r,this.selection=e}updateSelection(e,r){let i=this.selection;this.selection=e,this._selectionChanged.next({selection:e,source:r,oldValue:i})}ngOnDestroy(){this._selectionChanged.complete()}_isValidDateInstance(e){return this._adapter.isDateInstance(e)&&this._adapter.isValid(e)}static \u0275fac=function(r){jc()};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})(),OR=(()=>{class t extends Us{constructor(e){super(null,e)}add(e){super.updateSelection(e,this)}isValid(){return this.selection!=null&&this._isValidDateInstance(this.selection)}isComplete(){return this.selection!=null}clone(){let e=new t(this._adapter);return e.updateSelection(this.selection,this),e}static \u0275fac=function(r){return new(r||t)(X(_t))};static \u0275prov=me({token:t,factory:t.\u0275fac})}return t})();var fD={provide:Us,useFactory:()=>u(Us,{optional:!0,skipSelf:!0})||new OR(u(_t))};var pD=new b("MAT_DATE_RANGE_SELECTION_STRATEGY");var hg=7,FR=0,cD=(()=>{class t{_changeDetectorRef=u(ve);_dateFormats=u(Ar,{optional:!0});_dateAdapter=u(_t,{optional:!0});_dir=u(Tt,{optional:!0});_rangeStrategy=u(pD,{optional:!0});_rerenderSubscription=ue.EMPTY;_selectionKeyPressed=!1;get activeDate(){return this._activeDate}set activeDate(e){let r=this._activeDate,i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))||this._dateAdapter.today();this._activeDate=this._dateAdapter.clampDate(i,this.minDate,this.maxDate),this._hasSameMonthAndYear(r,this._activeDate)||this._init()}_activeDate;get selected(){return this._selected}set selected(e){e instanceof sn?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e)),this._setRanges(this._selected)}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;comparisonStart=null;comparisonEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;activeDrag=null;selectedChange=new C;_userSelection=new C;dragStarted=new C;dragEnded=new C;activeDateChange=new C;_matCalendarBody;_monthLabel=A("");_weeks=A([]);_firstWeekOffset=A(0);_rangeStart=A(null);_rangeEnd=A(null);_comparisonRangeStart=A(null);_comparisonRangeEnd=A(null);_previewStart=A(null);_previewEnd=A(null);_isRange=A(!1);_todayDate=A(null);_weekdays=A([]);constructor(){u(Me).load(kr),this._activeDate=this._dateAdapter.today()}ngAfterContentInit(){this._rerenderSubscription=this._dateAdapter.localeChanges.pipe(Ht(null)).subscribe(()=>this._init())}ngOnChanges(e){let r=e.comparisonStart||e.comparisonEnd;r&&!r.firstChange&&this._setRanges(this.selected),e.activeDrag&&!this.activeDrag&&this._clearPreview()}ngOnDestroy(){this._rerenderSubscription.unsubscribe()}_dateSelected(e){let r=e.value,i=this._getDateFromDayOfMonth(r),o,a;this._selected instanceof sn?(o=this._getDateInCurrentMonth(this._selected.start),a=this._getDateInCurrentMonth(this._selected.end)):o=a=this._getDateInCurrentMonth(this._selected),(o!==r||a!==r)&&this.selectedChange.emit(i),this._userSelection.emit({value:i,event:e.event}),this._clearPreview(),this._changeDetectorRef.markForCheck()}_updateActiveDate(e){let r=e.value,i=this._activeDate;this.activeDate=this._getDateFromDayOfMonth(r),this._dateAdapter.compareDate(i,this.activeDate)&&this.activeDateChange.emit(this._activeDate)}_handleCalendarBodyKeydown(e){let r=this._activeDate,i=this._isRtl();switch(e.keyCode){case 37:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,i?1:-1);break;case 39:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,i?-1:1);break;case 38:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,-7);break;case 40:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,7);break;case 36:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,1-this._dateAdapter.getDate(this._activeDate));break;case 35:this.activeDate=this._dateAdapter.addCalendarDays(this._activeDate,this._dateAdapter.getNumDaysInMonth(this._activeDate)-this._dateAdapter.getDate(this._activeDate));break;case 33:this.activeDate=e.altKey?this._dateAdapter.addCalendarYears(this._activeDate,-1):this._dateAdapter.addCalendarMonths(this._activeDate,-1);break;case 34:this.activeDate=e.altKey?this._dateAdapter.addCalendarYears(this._activeDate,1):this._dateAdapter.addCalendarMonths(this._activeDate,1);break;case 13:case 32:this._selectionKeyPressed=!0,this._canSelect(this._activeDate)&&e.preventDefault();return;case 27:this._previewEnd()!=null&&!xn(e)&&(this._clearPreview(),this.activeDrag?this.dragEnded.emit({value:null,event:e}):(this.selectedChange.emit(null),this._userSelection.emit({value:null,event:e})),e.preventDefault(),e.stopPropagation());return;default:return}this._dateAdapter.compareDate(r,this.activeDate)&&(this.activeDateChange.emit(this.activeDate),this._focusActiveCellAfterViewChecked()),e.preventDefault()}_handleCalendarBodyKeyup(e){(e.keyCode===32||e.keyCode===13)&&(this._selectionKeyPressed&&this._canSelect(this._activeDate)&&this._dateSelected({value:this._dateAdapter.getDate(this._activeDate),event:e}),this._selectionKeyPressed=!1)}_init(){this._setRanges(this.selected),this._todayDate.set(this._getCellCompareValue(this._dateAdapter.today())),this._monthLabel.set(this._dateFormats.display.monthLabel?this._dateAdapter.format(this.activeDate,this._dateFormats.display.monthLabel):this._dateAdapter.getMonthNames("short")[this._dateAdapter.getMonth(this.activeDate)].toLocaleUpperCase());let e=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),this._dateAdapter.getMonth(this.activeDate),1);this._firstWeekOffset.set((hg+this._dateAdapter.getDayOfWeek(e)-this._dateAdapter.getFirstDayOfWeek())%hg),this._initWeekdays(),this._createWeekCells(),this._changeDetectorRef.markForCheck()}_focusActiveCell(e){this._matCalendarBody._focusActiveCell(e)}_focusActiveCellAfterViewChecked(){this._matCalendarBody._scheduleFocusActiveCellAfterViewChecked()}_previewChanged({event:e,value:r}){if(this._rangeStrategy){let i=r?r.rawValue:null,o=this._rangeStrategy.createPreview(i,this.selected,e);if(this._previewStart.set(this._getCellCompareValue(o.start)),this._previewEnd.set(this._getCellCompareValue(o.end)),this.activeDrag&&i){let a=this._rangeStrategy.createDrag?.(this.activeDrag.value,this.selected,i,e);a&&(this._previewStart.set(this._getCellCompareValue(a.start)),this._previewEnd.set(this._getCellCompareValue(a.end)))}}}_dragEnded(e){if(this.activeDrag)if(e.value){let r=this._rangeStrategy?.createDrag?.(this.activeDrag.value,this.selected,e.value,e.event);this.dragEnded.emit({value:r??null,event:e.event})}else this.dragEnded.emit({value:null,event:e.event})}_getDateFromDayOfMonth(e){return this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),this._dateAdapter.getMonth(this.activeDate),e)}_initWeekdays(){let e=this._dateAdapter.getFirstDayOfWeek(),r=this._dateAdapter.getDayOfWeekNames("narrow"),o=this._dateAdapter.getDayOfWeekNames("long").map((a,s)=>({long:a,narrow:r[s],id:FR++}));this._weekdays.set(o.slice(e).concat(o.slice(0,e)))}_createWeekCells(){let e=this._dateAdapter.getNumDaysInMonth(this.activeDate),r=this._dateAdapter.getDateNames(),i=[[]];for(let o=0,a=this._firstWeekOffset();o<e;o++,a++){a==hg&&(i.push([]),a=0);let s=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),this._dateAdapter.getMonth(this.activeDate),o+1),l=this._shouldEnableDate(s),c=this._dateAdapter.format(s,this._dateFormats.display.dateA11yLabel),d=this.dateClass?this.dateClass(s,"month"):void 0;i[i.length-1].push(new Hs(o+1,r[o],c,l,d,this._getCellCompareValue(s),s))}this._weeks.set(i)}_shouldEnableDate(e){return!!e&&(!this.minDate||this._dateAdapter.compareDate(e,this.minDate)>=0)&&(!this.maxDate||this._dateAdapter.compareDate(e,this.maxDate)<=0)&&(!this.dateFilter||this.dateFilter(e))}_getDateInCurrentMonth(e){return e&&this._hasSameMonthAndYear(e,this.activeDate)?this._dateAdapter.getDate(e):null}_hasSameMonthAndYear(e,r){return!!(e&&r&&this._dateAdapter.getMonth(e)==this._dateAdapter.getMonth(r)&&this._dateAdapter.getYear(e)==this._dateAdapter.getYear(r))}_getCellCompareValue(e){if(e){let r=this._dateAdapter.getYear(e),i=this._dateAdapter.getMonth(e),o=this._dateAdapter.getDate(e);return new Date(r,i,o).getTime()}return null}_isRtl(){return this._dir&&this._dir.value==="rtl"}_setRanges(e){e instanceof sn?(this._rangeStart.set(this._getCellCompareValue(e.start)),this._rangeEnd.set(this._getCellCompareValue(e.end)),this._isRange.set(!0)):(this._rangeStart.set(this._getCellCompareValue(e)),this._rangeEnd.set(this._rangeStart()),this._isRange.set(!1)),this._comparisonRangeStart.set(this._getCellCompareValue(this.comparisonStart)),this._comparisonRangeEnd.set(this._getCellCompareValue(this.comparisonEnd))}_canSelect(e){return!this.dateFilter||this.dateFilter(e)}_clearPreview(){this._previewStart.set(null),this._previewEnd.set(null)}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=(i,o)=>o.id;function r(i,o){if(i&1&&(v(0,"th",2)(1,"span",6),M(2),y(),v(3,"span",3),M(4),y()()),i&2){let a=o.$implicit;_(2),Qt(a.long),_(2),Qt(a.narrow)}}return k({type:t,selectors:[["mat-month-view"]],viewQuery:function(o,a){if(o&1&&He(Xo,5),o&2){let s;ae(s=se())&&(a._matCalendarBody=s.first)}},inputs:{activeDate:"activeDate",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd",startDateAccessibleName:"startDateAccessibleName",endDateAccessibleName:"endDateAccessibleName",activeDrag:"activeDrag"},outputs:{selectedChange:"selectedChange",_userSelection:"_userSelection",dragStarted:"dragStarted",dragEnded:"dragEnded",activeDateChange:"activeDateChange"},exportAs:["matMonthView"],features:[Ae],decls:8,vars:14,consts:[["role","grid",1,"mat-calendar-table"],[1,"mat-calendar-table-header"],["scope","col"],["aria-hidden","true"],["colspan","7",1,"mat-calendar-table-header-divider"],["mat-calendar-body","",3,"selectedValueChange","activeDateChange","previewChange","dragStarted","dragEnded","keyup","keydown","label","rows","todayValue","startValue","endValue","comparisonStart","comparisonEnd","previewStart","previewEnd","isRange","labelMinRequiredCells","activeCell","startDateAccessibleName","endDateAccessibleName"],[1,"cdk-visually-hidden"]],template:function(o,a){o&1&&(v(0,"table",0)(1,"thead",1)(2,"tr"),pi(3,r,5,2,"th",2,e),y(),v(5,"tr",3),Z(6,"th",4),y()(),v(7,"tbody",5),B("selectedValueChange",function(l){return a._dateSelected(l)})("activeDateChange",function(l){return a._updateActiveDate(l)})("previewChange",function(l){return a._previewChanged(l)})("dragStarted",function(l){return a.dragStarted.emit(l)})("dragEnded",function(l){return a._dragEnded(l)})("keyup",function(l){return a._handleCalendarBodyKeyup(l)})("keydown",function(l){return a._handleCalendarBodyKeydown(l)}),y()()),o&2&&(_(3),hi(a._weekdays()),_(4),z("label",a._monthLabel())("rows",a._weeks())("todayValue",a._todayDate())("startValue",a._rangeStart())("endValue",a._rangeEnd())("comparisonStart",a._comparisonRangeStart())("comparisonEnd",a._comparisonRangeEnd())("previewStart",a._previewStart())("previewEnd",a._previewEnd())("isRange",a._isRange())("labelMinRequiredCells",3)("activeCell",a._dateAdapter.getDate(a.activeDate)-1)("startDateAccessibleName",a.startDateAccessibleName)("endDateAccessibleName",a.endDateAccessibleName))},dependencies:[Xo],encapsulation:2})})()}return t})(),zt=24,gg=4,dD=(()=>{class t{_changeDetectorRef=u(ve);_dateAdapter=u(_t,{optional:!0});_dir=u(Tt,{optional:!0});_rerenderSubscription=ue.EMPTY;_selectionKeyPressed=!1;get activeDate(){return this._activeDate}set activeDate(e){let r=this._activeDate,i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))||this._dateAdapter.today();this._activeDate=this._dateAdapter.clampDate(i,this.minDate,this.maxDate),hD(this._dateAdapter,r,this._activeDate,this.minDate,this.maxDate)||this._init()}_activeDate;get selected(){return this._selected}set selected(e){e instanceof sn?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e)),this._setSelectedYear(e)}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;selectedChange=new C;yearSelected=new C;activeDateChange=new C;_matCalendarBody;_years=A([]);_todayYear=A(0);_selectedYear=A(null);constructor(){this._dateAdapter,this._activeDate=this._dateAdapter.today()}ngAfterContentInit(){this._rerenderSubscription=this._dateAdapter.localeChanges.pipe(Ht(null)).subscribe(()=>this._init())}ngOnDestroy(){this._rerenderSubscription.unsubscribe()}_init(){this._todayYear.set(this._dateAdapter.getYear(this._dateAdapter.today()));let r=this._dateAdapter.getYear(this._activeDate)-zs(this._dateAdapter,this.activeDate,this.minDate,this.maxDate),i=[];for(let o=0,a=[];o<zt;o++)a.push(r+o),a.length==gg&&(i.push(a.map(s=>this._createCellForYear(s))),a=[]);this._years.set(i),this._changeDetectorRef.markForCheck()}_yearSelected(e){let r=e.value,i=this._dateAdapter.createDate(r,0,1),o=this._getDateFromYear(r);this.yearSelected.emit(i),this.selectedChange.emit(o)}_updateActiveDate(e){let r=e.value,i=this._activeDate;this.activeDate=this._getDateFromYear(r),this._dateAdapter.compareDate(i,this.activeDate)&&this.activeDateChange.emit(this.activeDate)}_handleCalendarBodyKeydown(e){let r=this._activeDate,i=this._isRtl();switch(e.keyCode){case 37:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,i?1:-1);break;case 39:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,i?-1:1);break;case 38:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,-gg);break;case 40:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,gg);break;case 36:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,-zs(this._dateAdapter,this.activeDate,this.minDate,this.maxDate));break;case 35:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,zt-zs(this._dateAdapter,this.activeDate,this.minDate,this.maxDate)-1);break;case 33:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?-zt*10:-zt);break;case 34:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?zt*10:zt);break;case 13:case 32:this._selectionKeyPressed=!0;break;default:return}this._dateAdapter.compareDate(r,this.activeDate)&&this.activeDateChange.emit(this.activeDate),this._focusActiveCellAfterViewChecked(),e.preventDefault()}_handleCalendarBodyKeyup(e){(e.keyCode===32||e.keyCode===13)&&(this._selectionKeyPressed&&this._yearSelected({value:this._dateAdapter.getYear(this._activeDate),event:e}),this._selectionKeyPressed=!1)}_getActiveCell(){return zs(this._dateAdapter,this.activeDate,this.minDate,this.maxDate)}_focusActiveCell(){this._matCalendarBody._focusActiveCell()}_focusActiveCellAfterViewChecked(){this._matCalendarBody._scheduleFocusActiveCellAfterViewChecked()}_getDateFromYear(e){let r=this._dateAdapter.getMonth(this.activeDate),i=this._dateAdapter.getNumDaysInMonth(this._dateAdapter.createDate(e,r,1));return this._dateAdapter.createDate(e,r,Math.min(this._dateAdapter.getDate(this.activeDate),i))}_createCellForYear(e){let r=this._dateAdapter.createDate(e,0,1),i=this._dateAdapter.getYearName(r),o=this.dateClass?this.dateClass(r,"multi-year"):void 0;return new Hs(e,i,i,this._shouldEnableYear(e),o)}_shouldEnableYear(e){if(e==null||this.maxDate&&e>this._dateAdapter.getYear(this.maxDate)||this.minDate&&e<this._dateAdapter.getYear(this.minDate))return!1;if(!this.dateFilter)return!0;let r=this._dateAdapter.createDate(e,0,1);for(let i=r;this._dateAdapter.getYear(i)==e;i=this._dateAdapter.addCalendarDays(i,1))if(this.dateFilter(i))return!0;return!1}_isRtl(){return this._dir&&this._dir.value==="rtl"}_setSelectedYear(e){if(this._selectedYear.set(null),e instanceof sn){let r=e.start||e.end;r&&this._selectedYear.set(this._dateAdapter.getYear(r))}else e&&this._selectedYear.set(this._dateAdapter.getYear(e))}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["mat-multi-year-view"]],viewQuery:function(r,i){if(r&1&&He(Xo,5),r&2){let o;ae(o=se())&&(i._matCalendarBody=o.first)}},inputs:{activeDate:"activeDate",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass"},outputs:{selectedChange:"selectedChange",yearSelected:"yearSelected",activeDateChange:"activeDateChange"},exportAs:["matMultiYearView"],decls:5,vars:7,consts:[["role","grid",1,"mat-calendar-table"],["aria-hidden","true",1,"mat-calendar-table-header"],["colspan","4",1,"mat-calendar-table-header-divider"],["mat-calendar-body","",3,"selectedValueChange","activeDateChange","keyup","keydown","rows","todayValue","startValue","endValue","numCols","cellAspectRatio","activeCell"]],template:function(r,i){r&1&&(v(0,"table",0)(1,"thead",1)(2,"tr"),Z(3,"th",2),y()(),v(4,"tbody",3),B("selectedValueChange",function(a){return i._yearSelected(a)})("activeDateChange",function(a){return i._updateActiveDate(a)})("keyup",function(a){return i._handleCalendarBodyKeyup(a)})("keydown",function(a){return i._handleCalendarBodyKeydown(a)}),y()()),r&2&&(_(4),z("rows",i._years())("todayValue",i._todayYear())("startValue",i._selectedYear())("endValue",i._selectedYear())("numCols",4)("cellAspectRatio",4/7)("activeCell",i._getActiveCell()))},dependencies:[Xo],encapsulation:2})}return t})();function hD(t,n,e,r,i){let o=t.getYear(n),a=t.getYear(e),s=gD(t,r,i);return Math.floor((o-s)/zt)===Math.floor((a-s)/zt)}function zs(t,n,e,r){let i=t.getYear(n);return PR(i-gD(t,e,r),zt)}function gD(t,n,e){let r=0;return e?r=t.getYear(e)-zt+1:n&&(r=t.getYear(n)),r}function PR(t,n){return(t%n+n)%n}var uD=(()=>{class t{_changeDetectorRef=u(ve);_dateFormats=u(Ar,{optional:!0});_dateAdapter=u(_t,{optional:!0});_dir=u(Tt,{optional:!0});_rerenderSubscription=ue.EMPTY;_selectionKeyPressed=!1;get activeDate(){return this._activeDate}set activeDate(e){let r=this._activeDate,i=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))||this._dateAdapter.today();this._activeDate=this._dateAdapter.clampDate(i,this.minDate,this.maxDate),this._dateAdapter.getYear(r)!==this._dateAdapter.getYear(this._activeDate)&&this._init()}_activeDate;get selected(){return this._selected}set selected(e){e instanceof sn?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e)),this._setSelectedMonth(e)}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;selectedChange=new C;monthSelected=new C;activeDateChange=new C;_matCalendarBody;_months=A([]);_yearLabel=A("");_todayMonth=A(null);_selectedMonth=A(null);constructor(){this._activeDate=this._dateAdapter.today()}ngAfterContentInit(){this._rerenderSubscription=this._dateAdapter.localeChanges.pipe(Ht(null)).subscribe(()=>this._init())}ngOnDestroy(){this._rerenderSubscription.unsubscribe()}_monthSelected(e){let r=e.value,i=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),r,1);this.monthSelected.emit(i);let o=this._getDateFromMonth(r);this.selectedChange.emit(o)}_updateActiveDate(e){let r=e.value,i=this._activeDate;this.activeDate=this._getDateFromMonth(r),this._dateAdapter.compareDate(i,this.activeDate)&&this.activeDateChange.emit(this.activeDate)}_handleCalendarBodyKeydown(e){let r=this._activeDate,i=this._isRtl();switch(e.keyCode){case 37:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,i?1:-1);break;case 39:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,i?-1:1);break;case 38:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,-4);break;case 40:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,4);break;case 36:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,-this._dateAdapter.getMonth(this._activeDate));break;case 35:this.activeDate=this._dateAdapter.addCalendarMonths(this._activeDate,11-this._dateAdapter.getMonth(this._activeDate));break;case 33:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?-10:-1);break;case 34:this.activeDate=this._dateAdapter.addCalendarYears(this._activeDate,e.altKey?10:1);break;case 13:case 32:this._selectionKeyPressed=!0;break;default:return}this._dateAdapter.compareDate(r,this.activeDate)&&(this.activeDateChange.emit(this.activeDate),this._focusActiveCellAfterViewChecked()),e.preventDefault()}_handleCalendarBodyKeyup(e){(e.keyCode===32||e.keyCode===13)&&(this._selectionKeyPressed&&this._monthSelected({value:this._dateAdapter.getMonth(this._activeDate),event:e}),this._selectionKeyPressed=!1)}_init(){this._setSelectedMonth(this.selected),this._todayMonth.set(this._getMonthInCurrentYear(this._dateAdapter.today())),this._yearLabel.set(this._dateAdapter.getYearName(this.activeDate));let e=this._dateAdapter.getMonthNames("short");this._months.set([[0,1,2,3],[4,5,6,7],[8,9,10,11]].map(r=>r.map(i=>this._createCellForMonth(i,e[i])))),this._changeDetectorRef.markForCheck()}_focusActiveCell(){this._matCalendarBody._focusActiveCell()}_focusActiveCellAfterViewChecked(){this._matCalendarBody._scheduleFocusActiveCellAfterViewChecked()}_getMonthInCurrentYear(e){return e&&this._dateAdapter.getYear(e)==this._dateAdapter.getYear(this.activeDate)?this._dateAdapter.getMonth(e):null}_getDateFromMonth(e){let r=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),e,1),i=this._dateAdapter.getNumDaysInMonth(r);return this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),e,Math.min(this._dateAdapter.getDate(this.activeDate),i))}_createCellForMonth(e,r){let i=this._dateAdapter.createDate(this._dateAdapter.getYear(this.activeDate),e,1),o=this._dateAdapter.format(i,this._dateFormats.display.monthYearA11yLabel),a=this.dateClass?this.dateClass(i,"year"):void 0;return new Hs(e,r.toLocaleUpperCase(),o,this._shouldEnableMonth(e),a)}_shouldEnableMonth(e){let r=this._dateAdapter.getYear(this.activeDate);if(e==null||this._isYearAndMonthAfterMaxDate(r,e)||this._isYearAndMonthBeforeMinDate(r,e))return!1;if(!this.dateFilter)return!0;let i=this._dateAdapter.createDate(r,e,1);for(let o=i;this._dateAdapter.getMonth(o)==e;o=this._dateAdapter.addCalendarDays(o,1))if(this.dateFilter(o))return!0;return!1}_isYearAndMonthAfterMaxDate(e,r){if(this.maxDate){let i=this._dateAdapter.getYear(this.maxDate),o=this._dateAdapter.getMonth(this.maxDate);return e>i||e===i&&r>o}return!1}_isYearAndMonthBeforeMinDate(e,r){if(this.minDate){let i=this._dateAdapter.getYear(this.minDate),o=this._dateAdapter.getMonth(this.minDate);return e<i||e===i&&r<o}return!1}_isRtl(){return this._dir&&this._dir.value==="rtl"}_setSelectedMonth(e){e instanceof sn?this._selectedMonth.set(this._getMonthInCurrentYear(e.start)||this._getMonthInCurrentYear(e.end)):this._selectedMonth.set(this._getMonthInCurrentYear(e))}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["mat-year-view"]],viewQuery:function(r,i){if(r&1&&He(Xo,5),r&2){let o;ae(o=se())&&(i._matCalendarBody=o.first)}},inputs:{activeDate:"activeDate",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass"},outputs:{selectedChange:"selectedChange",monthSelected:"monthSelected",activeDateChange:"activeDateChange"},exportAs:["matYearView"],decls:5,vars:9,consts:[["role","grid",1,"mat-calendar-table"],["aria-hidden","true",1,"mat-calendar-table-header"],["colspan","4",1,"mat-calendar-table-header-divider"],["mat-calendar-body","",3,"selectedValueChange","activeDateChange","keyup","keydown","label","rows","todayValue","startValue","endValue","labelMinRequiredCells","numCols","cellAspectRatio","activeCell"]],template:function(r,i){r&1&&(v(0,"table",0)(1,"thead",1)(2,"tr"),Z(3,"th",2),y()(),v(4,"tbody",3),B("selectedValueChange",function(a){return i._monthSelected(a)})("activeDateChange",function(a){return i._updateActiveDate(a)})("keyup",function(a){return i._handleCalendarBodyKeyup(a)})("keydown",function(a){return i._handleCalendarBodyKeydown(a)}),y()()),r&2&&(_(4),z("label",i._yearLabel())("rows",i._months())("todayValue",i._todayMonth())("startValue",i._selectedMonth())("endValue",i._selectedMonth())("labelMinRequiredCells",2)("numCols",4)("cellAspectRatio",4/7)("activeCell",i._dateAdapter.getMonth(i.activeDate)))},dependencies:[Xo],encapsulation:2})}return t})(),bD=(()=>{class t{_intl=u(ta);calendar=u(bg);_dateAdapter=u(_t,{optional:!0});_dateFormats=u(Ar,{optional:!0});_periodButtonText;_periodButtonDescription;_periodButtonLabel;_prevButtonLabel;_nextButtonLabel;constructor(){u(Me).load(kr);let e=u(ve);this._updateLabels(),this.calendar.stateChanges.subscribe(()=>{this._updateLabels(),e.markForCheck()})}get periodButtonText(){return this._periodButtonText}get periodButtonDescription(){return this._periodButtonDescription}get periodButtonLabel(){return this._periodButtonLabel}get prevButtonLabel(){return this._prevButtonLabel}get nextButtonLabel(){return this._nextButtonLabel}currentPeriodClicked(){this.calendar.currentView=this.calendar.currentView=="month"?"multi-year":"month"}previousClicked(){this.previousEnabled()&&(this.calendar.activeDate=this.calendar.currentView=="month"?this._dateAdapter.addCalendarMonths(this.calendar.activeDate,-1):this._dateAdapter.addCalendarYears(this.calendar.activeDate,this.calendar.currentView=="year"?-1:-zt))}nextClicked(){this.nextEnabled()&&(this.calendar.activeDate=this.calendar.currentView=="month"?this._dateAdapter.addCalendarMonths(this.calendar.activeDate,1):this._dateAdapter.addCalendarYears(this.calendar.activeDate,this.calendar.currentView=="year"?1:zt))}previousEnabled(){return this.calendar.minDate?!this.calendar.minDate||!this._isSameView(this.calendar.activeDate,this.calendar.minDate):!0}nextEnabled(){return!this.calendar.maxDate||!this._isSameView(this.calendar.activeDate,this.calendar.maxDate)}_updateLabels(){let e=this.calendar,r=this._intl,i=this._dateAdapter;e.currentView==="month"?(this._periodButtonText=i.format(e.activeDate,this._dateFormats.display.monthYearLabel).toLocaleUpperCase(),this._periodButtonDescription=i.format(e.activeDate,this._dateFormats.display.monthYearLabel).toLocaleUpperCase(),this._periodButtonLabel=r.switchToMultiYearViewLabel,this._prevButtonLabel=r.prevMonthLabel,this._nextButtonLabel=r.nextMonthLabel):e.currentView==="year"?(this._periodButtonText=i.getYearName(e.activeDate),this._periodButtonDescription=i.getYearName(e.activeDate),this._periodButtonLabel=r.switchToMonthViewLabel,this._prevButtonLabel=r.prevYearLabel,this._nextButtonLabel=r.nextYearLabel):(this._periodButtonText=r.formatYearRange(...this._formatMinAndMaxYearLabels()),this._periodButtonDescription=r.formatYearRangeLabel(...this._formatMinAndMaxYearLabels()),this._periodButtonLabel=r.switchToMonthViewLabel,this._prevButtonLabel=r.prevMultiYearLabel,this._nextButtonLabel=r.nextMultiYearLabel)}_isSameView(e,r){return this.calendar.currentView=="month"?this._dateAdapter.getYear(e)==this._dateAdapter.getYear(r)&&this._dateAdapter.getMonth(e)==this._dateAdapter.getMonth(r):this.calendar.currentView=="year"?this._dateAdapter.getYear(e)==this._dateAdapter.getYear(r):hD(this._dateAdapter,e,r,this.calendar.minDate,this.calendar.maxDate)}_formatMinAndMaxYearLabels(){let r=this._dateAdapter.getYear(this.calendar.activeDate)-zs(this._dateAdapter,this.calendar.activeDate,this.calendar.minDate,this.calendar.maxDate),i=r+zt-1,o=this._dateAdapter.getYearName(this._dateAdapter.createDate(r,0,1)),a=this._dateAdapter.getYearName(this._dateAdapter.createDate(i,0,1));return[o,a]}_periodButtonLabelId=u(je).getId("mat-calendar-period-label-");static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){return k({type:t,selectors:[["mat-calendar-header"]],exportAs:["matCalendarHeader"],ngContentSelectors:["*"],decls:17,vars:13,consts:[[1,"mat-calendar-header"],[1,"mat-calendar-controls"],["aria-live","polite",1,"cdk-visually-hidden",3,"id"],["matButton","","type","button",1,"mat-calendar-period-button",3,"click"],["aria-hidden","true"],["viewBox","0 0 10 5","focusable","false","aria-hidden","true",1,"mat-calendar-arrow"],["points","0,0 5,5 10,0"],[1,"mat-calendar-spacer"],["matIconButton","","type","button","disabledInteractive","",1,"mat-calendar-previous-button",3,"click","disabled","matTooltip"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","disabledInteractive","",1,"mat-calendar-next-button",3,"click","disabled","matTooltip"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"]],template:function(i,o){i&1&&(Oe(),v(0,"div",0)(1,"div",1)(2,"span",2),M(3),y(),v(4,"button",3),B("click",function(){return o.currentPeriodClicked()}),v(5,"span",4),M(6),y(),un(),v(7,"svg",5),Z(8,"polygon",6),y()(),si(),Z(9,"div",7),ce(10),v(11,"button",8),B("click",function(){return o.previousClicked()}),un(),v(12,"svg",9),Z(13,"path",10),y()(),si(),v(14,"button",11),B("click",function(){return o.nextClicked()}),un(),v(15,"svg",9),Z(16,"path",12),y()()()()),i&2&&(_(2),z("id",o._periodButtonLabelId),_(),Qt(o.periodButtonDescription),_(),de("aria-label",o.periodButtonLabel)("aria-describedby",o._periodButtonLabelId),_(2),Qt(o.periodButtonText),_(),U("mat-calendar-invert",o.calendar.currentView!=="month"),_(4),z("disabled",!o.previousEnabled())("matTooltip",o.prevButtonLabel),de("aria-label",o.prevButtonLabel),_(3),z("disabled",!o.nextEnabled())("matTooltip",o.nextButtonLabel),de("aria-label",o.nextButtonLabel))},dependencies:[nr,Md,aD],encapsulation:2})})()}return t})(),bg=(()=>{class t{_dateAdapter=u(_t,{optional:!0});_dateFormats=u(Ar,{optional:!0});_changeDetectorRef=u(ve);_elementRef=u(V);headerComponent;_calendarHeaderPortal;_intlChanges;_moveFocusOnNextTick=!1;get startAt(){return this._startAt}set startAt(e){this._startAt=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_startAt=null;startView="month";get selected(){return this._selected}set selected(e){e instanceof sn?this._selected=e:this._selected=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_selected=null;get minDate(){return this._minDate}set minDate(e){this._minDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_minDate=null;get maxDate(){return this._maxDate}set maxDate(e){this._maxDate=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_maxDate=null;dateFilter;dateClass;comparisonStart=null;comparisonEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;selectedChange=new C;yearSelected=new C;monthSelected=new C;viewChanged=new C(!0);_userSelection=new C;_userDragDrop=new C;monthView;yearView;multiYearView;get activeDate(){return this._clampedActiveDate}set activeDate(e){this._clampedActiveDate=this._dateAdapter.clampDate(e,this.minDate,this.maxDate),this.stateChanges.next(),this._changeDetectorRef.markForCheck()}_clampedActiveDate;get currentView(){return this._currentView}set currentView(e){let r=this._currentView!==e?e:null;this._currentView=e,this._moveFocusOnNextTick=!0,this._changeDetectorRef.markForCheck(),r&&(this.stateChanges.next(),this.viewChanged.emit(r))}_currentView;_activeDrag=null;stateChanges=new E;constructor(){this._intlChanges=u(ta).changes.subscribe(()=>{this._changeDetectorRef.markForCheck(),this.stateChanges.next()})}ngAfterContentInit(){this._calendarHeaderPortal=new Rr(this.headerComponent||bD),this.activeDate=this.startAt||this._dateAdapter.today(),this._currentView=this.startView}ngAfterViewChecked(){this._moveFocusOnNextTick&&(this._moveFocusOnNextTick=!1,this.focusActiveCell())}ngOnDestroy(){this._intlChanges.unsubscribe(),this.stateChanges.complete()}ngOnChanges(e){let r=e.minDate&&!this._dateAdapter.sameDate(e.minDate.previousValue,e.minDate.currentValue)?e.minDate:void 0,i=e.maxDate&&!this._dateAdapter.sameDate(e.maxDate.previousValue,e.maxDate.currentValue)?e.maxDate:void 0,o=r||i||e.dateFilter;if(o&&!o.firstChange){let a=this._getCurrentViewComponent();a&&(this._elementRef.nativeElement.contains(Mo())&&(this._moveFocusOnNextTick=!0),this._changeDetectorRef.detectChanges(),a._init())}this.stateChanges.next()}focusActiveCell(){this._getCurrentViewComponent()?._focusActiveCell(!1)}updateTodaysDate(){this._getCurrentViewComponent()?._init()}_dateSelected(e){let r=e.value;(this.selected instanceof sn||r&&!this._dateAdapter.sameDate(r,this.selected))&&this.selectedChange.emit(r),this._userSelection.emit(e)}_yearSelectedInMultiYearView(e){this.yearSelected.emit(e)}_monthSelectedInYearView(e){this.monthSelected.emit(e)}_goToDateInView(e,r){this.activeDate=e,this.currentView=r}_dragStarted(e){this._activeDrag=e}_dragEnded(e){this._activeDrag&&(e.value&&this._userDragDrop.emit(e),this._activeDrag=null)}_getCurrentViewComponent(){return this.monthView||this.yearView||this.multiYearView}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){function e(a,s){}function r(a,s){if(a&1){let l=xr();v(0,"mat-month-view",4),uo("activeDateChange",function(d){dt(l);let m=fe();return Xa(m.activeDate,d)||(m.activeDate=d),ut(d)}),B("_userSelection",function(d){dt(l);let m=fe();return ut(m._dateSelected(d))})("dragStarted",function(d){dt(l);let m=fe();return ut(m._dragStarted(d))})("dragEnded",function(d){dt(l);let m=fe();return ut(m._dragEnded(d))}),y()}if(a&2){let l=fe();co("activeDate",l.activeDate),z("selected",l.selected)("dateFilter",l.dateFilter)("maxDate",l.maxDate)("minDate",l.minDate)("dateClass",l.dateClass)("comparisonStart",l.comparisonStart)("comparisonEnd",l.comparisonEnd)("startDateAccessibleName",l.startDateAccessibleName)("endDateAccessibleName",l.endDateAccessibleName)("activeDrag",l._activeDrag)}}function i(a,s){if(a&1){let l=xr();v(0,"mat-year-view",5),uo("activeDateChange",function(d){dt(l);let m=fe();return Xa(m.activeDate,d)||(m.activeDate=d),ut(d)}),B("monthSelected",function(d){dt(l);let m=fe();return ut(m._monthSelectedInYearView(d))})("selectedChange",function(d){dt(l);let m=fe();return ut(m._goToDateInView(d,"month"))}),y()}if(a&2){let l=fe();co("activeDate",l.activeDate),z("selected",l.selected)("dateFilter",l.dateFilter)("maxDate",l.maxDate)("minDate",l.minDate)("dateClass",l.dateClass)}}function o(a,s){if(a&1){let l=xr();v(0,"mat-multi-year-view",6),uo("activeDateChange",function(d){dt(l);let m=fe();return Xa(m.activeDate,d)||(m.activeDate=d),ut(d)}),B("yearSelected",function(d){dt(l);let m=fe();return ut(m._yearSelectedInMultiYearView(d))})("selectedChange",function(d){dt(l);let m=fe();return ut(m._goToDateInView(d,"year"))}),y()}if(a&2){let l=fe();co("activeDate",l.activeDate),z("selected",l.selected)("dateFilter",l.dateFilter)("maxDate",l.maxDate)("minDate",l.minDate)("dateClass",l.dateClass)}}return k({type:t,selectors:[["mat-calendar"]],viewQuery:function(s,l){if(s&1&&He(cD,5)(uD,5)(dD,5),s&2){let c;ae(c=se())&&(l.monthView=c.first),ae(c=se())&&(l.yearView=c.first),ae(c=se())&&(l.multiYearView=c.first)}},hostAttrs:[1,"mat-calendar"],inputs:{headerComponent:"headerComponent",startAt:"startAt",startView:"startView",selected:"selected",minDate:"minDate",maxDate:"maxDate",dateFilter:"dateFilter",dateClass:"dateClass",comparisonStart:"comparisonStart",comparisonEnd:"comparisonEnd",startDateAccessibleName:"startDateAccessibleName",endDateAccessibleName:"endDateAccessibleName"},outputs:{selectedChange:"selectedChange",yearSelected:"yearSelected",monthSelected:"monthSelected",viewChanged:"viewChanged",_userSelection:"_userSelection",_userDragDrop:"_userDragDrop"},exportAs:["matCalendar"],features:[ke([fD]),Ae],decls:5,vars:2,consts:[[3,"cdkPortalOutlet"],["cdkMonitorSubtreeFocus","","tabindex","-1",1,"mat-calendar-content"],[3,"activeDate","selected","dateFilter","maxDate","minDate","dateClass","comparisonStart","comparisonEnd","startDateAccessibleName","endDateAccessibleName","activeDrag"],[3,"activeDate","selected","dateFilter","maxDate","minDate","dateClass"],[3,"activeDateChange","_userSelection","dragStarted","dragEnded","activeDate","selected","dateFilter","maxDate","minDate","dateClass","comparisonStart","comparisonEnd","startDateAccessibleName","endDateAccessibleName","activeDrag"],[3,"activeDateChange","monthSelected","selectedChange","activeDate","selected","dateFilter","maxDate","minDate","dateClass"],[3,"activeDateChange","yearSelected","selectedChange","activeDate","selected","dateFilter","maxDate","minDate","dateClass"]],template:function(s,l){if(s&1&&(Jn(0,e,0,0,"ng-template",0),v(1,"div",1),xe(2,r,1,11,"mat-month-view",2)(3,i,1,6,"mat-year-view",3)(4,o,1,6,"mat-multi-year-view",3),y()),s&2){let c;z("cdkPortalOutlet",l._calendarHeaderPortal),_(2),we((c=l.currentView)==="month"?2:c==="year"?3:c==="multi-year"?4:-1)}},dependencies:[ig,Ch,cD,uD,dD],styles:[`.mat-calendar {
  display: block;
  line-height: normal;
  font-family: var(--%NS%mat-datepicker-calendar-text-font, var(--%NS%mat-sys-body-medium-font));
  font-size: var(--%NS%mat-datepicker-calendar-text-size, var(--%NS%mat-sys-body-medium-size));
}

.mat-calendar-header {
  padding: 8px 8px 0 8px;
}

.mat-calendar-content {
  padding: 0 8px 8px 8px;
  outline: none;
}

.mat-calendar-controls {
  display: flex;
  align-items: center;
  margin: 5% calc(4.7142857143% - 16px);
}

.mat-calendar-spacer {
  flex: 1 1 auto;
}

button.mat-calendar-period-button {
  min-width: 0;
  margin: 0 8px;
  font-size: var(--%NS%mat-datepicker-calendar-period-button-text-size, var(--%NS%mat-sys-title-small-size));
  font-weight: var(--%NS%mat-datepicker-calendar-period-button-text-weight, var(--%NS%mat-sys-title-small-weight));
  --%NS%mat-button-text-label-text-color: var(--%NS%mat-datepicker-calendar-period-button-text-color, var(--%NS%mat-sys-on-surface-variant));
}

.mat-calendar-arrow {
  display: inline-block;
  width: 10px;
  height: 5px;
  margin: 0 0 0 5px;
  vertical-align: middle;
  fill: var(--%NS%mat-datepicker-calendar-period-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-calendar-arrow.mat-calendar-invert {
  transform: rotate(180deg);
}
[dir=rtl] .mat-calendar-arrow {
  margin: 0 5px 0 0;
}
@media (forced-colors: active) {
  .mat-calendar-arrow {
    fill: CanvasText;
  }
}

.mat-datepicker-content .mat-calendar-previous-button:not(.mat-mdc-button-disabled),
.mat-datepicker-content .mat-calendar-next-button:not(.mat-mdc-button-disabled) {
  color: var(--%NS%mat-datepicker-calendar-navigation-button-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
[dir=rtl] .mat-calendar-previous-button,
[dir=rtl] .mat-calendar-next-button {
  transform: rotate(180deg);
}

.mat-calendar-table {
  border-spacing: 0;
  border-collapse: collapse;
  width: 100%;
}

.mat-calendar-table-header th {
  text-align: center;
  padding: 0 0 8px 0;
  color: var(--%NS%mat-datepicker-calendar-header-text-color, var(--%NS%mat-sys-on-surface-variant));
  font-size: var(--%NS%mat-datepicker-calendar-header-text-size, var(--%NS%mat-sys-title-small-size));
  font-weight: var(--%NS%mat-datepicker-calendar-header-text-weight, var(--%NS%mat-sys-title-small-weight));
}

.mat-calendar-table-header-divider {
  position: relative;
  height: 1px;
}
.mat-calendar-table-header-divider::after {
  content: "";
  position: absolute;
  top: 0;
  left: -8px;
  right: -8px;
  height: 1px;
  background: var(--%NS%mat-datepicker-calendar-header-divider-color, transparent);
}

.mat-calendar-body-cell-content::before {
  margin: calc(calc(var(--%NS%mat-focus-indicator-border-width, 3px) + 3px) * -1);
}

.mat-calendar-body-cell:focus-visible .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2})})()}return t})(),LR=new b("mat-datepicker-scroll-strategy",{providedIn:"root",factory:()=>{let t=u(J);return()=>Yo(t)}}),vD=(()=>{class t{_elementRef=u(V);_animationsDisabled=Be();_changeDetectorRef=u(ve);_globalModel=u(Us);_dateAdapter=u(_t);_ngZone=u(T);_rangeSelectionStrategy=u(pD,{optional:!0});_stateChanges;_model;_eventCleanups;_animationFallback;_calendar;color;datepicker;comparisonStart=null;comparisonEnd=null;startDateAccessibleName=null;endDateAccessibleName=null;_isAbove=!1;_animationDone=new E;_isAnimating=!1;_closeButtonText;_closeButtonFocused=!1;_actionsPortal=null;_dialogLabelId=null;constructor(){if(u(Me).load(kr),this._closeButtonText=u(ta).closeCalendarLabel,!this._animationsDisabled){let e=this._elementRef.nativeElement,r=u(Te);this._eventCleanups=this._ngZone.runOutsideAngular(()=>[r.listen(e,"animationstart",this._handleAnimationEvent),r.listen(e,"animationend",this._handleAnimationEvent),r.listen(e,"animationcancel",this._handleAnimationEvent)])}}ngAfterViewInit(){this._stateChanges=this.datepicker.stateChanges.subscribe(()=>{this._changeDetectorRef.markForCheck()}),this._calendar.focusActiveCell()}ngOnDestroy(){clearTimeout(this._animationFallback),this._eventCleanups?.forEach(e=>e()),this._stateChanges?.unsubscribe(),this._animationDone.complete()}_handleUserSelection(e){let r=this._model.selection,i=e.value,o=r instanceof sn;if(o&&this._rangeSelectionStrategy){let a=this._rangeSelectionStrategy.selectionFinished(i,r,e.event);this._model.updateSelection(a,this)}else i&&(o||!this._dateAdapter.sameDate(i,r))&&this._model.add(i);(!this._model||this._model.isComplete())&&!this._actionsPortal&&this.datepicker.close()}_handleUserDragDrop(e){this._model.updateSelection(e.value,this)}_startExitAnimation(){this._elementRef.nativeElement.classList.add("mat-datepicker-content-exit"),this._animationsDisabled?this._animationDone.next():(clearTimeout(this._animationFallback),this._animationFallback=setTimeout(()=>{this._isAnimating||this._animationDone.next()},200))}_handleAnimationEvent=e=>{let r=this._elementRef.nativeElement;e.target!==r||!e.animationName.startsWith("_mat-datepicker-content")||(clearTimeout(this._animationFallback),this._isAnimating=e.type==="animationstart",r.classList.toggle("mat-datepicker-content-animating",this._isAnimating),this._isAnimating||this._animationDone.next())};_getSelected(){return this._model.selection}_applyPendingSelection(){this._model!==this._globalModel&&this._globalModel.updateSelection(this._model.selection,this)}_assignActions(e,r){this._model=e?this._globalModel.clone():this._globalModel,this._actionsPortal=e,r&&this._changeDetectorRef.detectChanges()}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){function e(r,i){}return k({type:t,selectors:[["mat-datepicker-content"]],viewQuery:function(i,o){if(i&1&&He(bg,5),i&2){let a;ae(a=se())&&(o._calendar=a.first)}},hostAttrs:[1,"mat-datepicker-content"],hostVars:6,hostBindings:function(i,o){i&2&&(Vt(o.color?"mat-"+o.color:""),U("mat-datepicker-content-touch",o.datepicker.touchUi)("mat-datepicker-content-animations-enabled",!o._animationsDisabled))},inputs:{color:"color"},exportAs:["matDatepickerContent"],decls:5,vars:26,consts:[["cdkTrapFocus","","role","dialog",1,"mat-datepicker-content-container"],[3,"yearSelected","monthSelected","viewChanged","_userSelection","_userDragDrop","id","startAt","startView","minDate","maxDate","dateFilter","headerComponent","selected","dateClass","comparisonStart","comparisonEnd","startDateAccessibleName","endDateAccessibleName"],[3,"cdkPortalOutlet"],["type","button","matButton","elevated",1,"mat-datepicker-close-button",3,"focus","blur","click","color"]],template:function(i,o){i&1&&(v(0,"div",0)(1,"mat-calendar",1),B("yearSelected",function(s){return o.datepicker._selectYear(s)})("monthSelected",function(s){return o.datepicker._selectMonth(s)})("viewChanged",function(s){return o.datepicker._viewChanged(s)})("_userSelection",function(s){return o._handleUserSelection(s)})("_userDragDrop",function(s){return o._handleUserDragDrop(s)}),y(),Jn(2,e,0,0,"ng-template",2),v(3,"button",3),B("focus",function(){return o._closeButtonFocused=!0})("blur",function(){return o._closeButtonFocused=!1})("click",function(){return o.datepicker.close()}),M(4),y()()),i&2&&(U("mat-datepicker-content-container-with-custom-header",o.datepicker.calendarHeaderComponent)("mat-datepicker-content-container-with-actions",o._actionsPortal),de("aria-modal",!0)("aria-labelledby",o._dialogLabelId??void 0),_(),Vt(o.datepicker.panelClass),z("id",o.datepicker.id)("startAt",o.datepicker.startAt)("startView",o.datepicker.startView)("minDate",o.datepicker._getMinDate())("maxDate",o.datepicker._getMaxDate())("dateFilter",o.datepicker._getDateFilter())("headerComponent",o.datepicker.calendarHeaderComponent)("selected",o._getSelected())("dateClass",o.datepicker.dateClass)("comparisonStart",o.comparisonStart)("comparisonEnd",o.comparisonEnd)("startDateAccessibleName",o.startDateAccessibleName)("endDateAccessibleName",o.endDateAccessibleName),_(),z("cdkPortalOutlet",o._actionsPortal),_(),U("cdk-visually-hidden",!o._closeButtonFocused),z("color",o.color||"primary"),_(),Qt(o._closeButtonText))},dependencies:[kh,bg,ig,nr],styles:[`@keyframes _mat-datepicker-content-dropdown-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-datepicker-content-dialog-enter {
  from {
    opacity: 0;
    transform: scale(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-datepicker-content-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-datepicker-content {
  display: block;
  background-color: var(--%NS%mat-datepicker-calendar-container-background-color, var(--%NS%mat-sys-surface-container-high));
  color: var(--%NS%mat-datepicker-calendar-container-text-color, var(--%NS%mat-sys-on-surface));
  box-shadow: var(--%NS%mat-datepicker-calendar-container-elevation-shadow, 0px 0px 0px 0px rgba(0, 0, 0, 0.2), 0px 0px 0px 0px rgba(0, 0, 0, 0.14), 0px 0px 0px 0px rgba(0, 0, 0, 0.12));
  border-radius: var(--%NS%mat-datepicker-calendar-container-shape, var(--%NS%mat-sys-corner-large));
}
.mat-datepicker-content.mat-datepicker-content-animations-enabled {
  animation: _mat-datepicker-content-dropdown-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-datepicker-content .mat-calendar {
  width: 296px;
  height: 354px;
}
.mat-datepicker-content .mat-datepicker-content-container-with-custom-header .mat-calendar {
  height: auto;
}
.mat-datepicker-content .mat-datepicker-close-button {
  position: absolute;
  top: 100%;
  left: 0;
  margin-top: 8px;
}
.mat-datepicker-content-animating .mat-datepicker-content .mat-datepicker-close-button {
  display: none;
}

.mat-datepicker-content-container {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.mat-datepicker-content-touch {
  display: block;
  max-height: 80vh;
  box-shadow: var(--%NS%mat-datepicker-calendar-container-touch-elevation-shadow, 0px 0px 0px 0px rgba(0, 0, 0, 0.2), 0px 0px 0px 0px rgba(0, 0, 0, 0.14), 0px 0px 0px 0px rgba(0, 0, 0, 0.12));
  border-radius: var(--%NS%mat-datepicker-calendar-container-touch-shape, var(--%NS%mat-sys-corner-extra-large));
  position: relative;
  overflow: visible;
  min-height: fit-content;
}
.mat-datepicker-content-touch.mat-datepicker-content-animations-enabled {
  animation: _mat-datepicker-content-dialog-enter 150ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-datepicker-content-touch .mat-datepicker-content-container {
  min-height: fit-content;
  max-height: 788px;
  min-width: 250px;
  max-width: 750px;
}
.mat-datepicker-content-touch .mat-calendar {
  width: 100%;
  height: auto;
}

.mat-datepicker-content-exit.mat-datepicker-content-animations-enabled {
  animation: _mat-datepicker-content-exit 100ms linear;
}

@media all and (orientation: landscape) {
  .mat-datepicker-content-touch .mat-datepicker-content-container {
    width: 64vh;
    height: 80vh;
  }
}
@media all and (orientation: portrait) {
  .mat-datepicker-content-touch .mat-datepicker-content-container {
    width: 80vw;
    height: 100vw;
  }
}
`],encapsulation:2})})()}return t})(),mD=(()=>{class t{_injector=u(J);_viewContainerRef=u(bt);_dateAdapter=u(_t,{optional:!0});_dir=u(Tt,{optional:!0});_model=u(Us);_animationsDisabled=Be();_scrollStrategy=u(LR);_inputStateChanges=ue.EMPTY;_document=u(G);calendarHeaderComponent;get startAt(){return this._startAt||(this.datepickerInput?this.datepickerInput.getStartValue():null)}set startAt(e){this._startAt=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e))}_startAt=null;startView="month";get color(){return this._color||(this.datepickerInput?this.datepickerInput.getThemePalette():void 0)}set color(e){this._color=e}_color;touchUi=!1;get disabled(){return this._disabled===void 0&&this.datepickerInput?this.datepickerInput.disabled:!!this._disabled}set disabled(e){e!==this._disabled&&(this._disabled=e,this.stateChanges.next(void 0))}_disabled;xPosition="start";yPosition="below";restoreFocus=!0;yearSelected=new C;monthSelected=new C;viewChanged=new C(!0);dateClass;openedStream=new C;closedStream=new C;get panelClass(){return this._panelClass}set panelClass(e){this._panelClass=zw(e)}_panelClass;get opened(){return this._opened}set opened(e){e?this.open():this.close()}_opened=!1;id=u(je).getId("mat-datepicker-");_getMinDate(){return this.datepickerInput&&this.datepickerInput.min}_getMaxDate(){return this.datepickerInput&&this.datepickerInput.max}_getDateFilter(){return this.datepickerInput&&this.datepickerInput.dateFilter}_overlayRef=null;_componentRef=null;_focusedElementBeforeOpen=null;_backdropHarnessClass=`${this.id}-backdrop`;_actionsPortal=null;datepickerInput;stateChanges=new E;_changeDetectorRef=u(ve);constructor(){this._dateAdapter,this._model.selectionChanged.subscribe(()=>{this._changeDetectorRef.markForCheck()})}ngOnChanges(e){let r=e.xPosition||e.yPosition;if(r&&!r.firstChange&&this._overlayRef){let i=this._overlayRef.getConfig().positionStrategy;i instanceof qo&&(this._setConnectedPositions(i),this.opened&&this._overlayRef.updatePosition())}this.stateChanges.next(void 0)}ngOnDestroy(){this._destroyOverlay(),this.close(),this._inputStateChanges.unsubscribe(),this.stateChanges.complete()}select(e){this._model.add(e)}_selectYear(e){this.yearSelected.emit(e)}_selectMonth(e){this.monthSelected.emit(e)}_viewChanged(e){this.viewChanged.emit(e)}registerInput(e){return this.datepickerInput,this._inputStateChanges.unsubscribe(),this.datepickerInput=e,this._inputStateChanges=e.stateChanges.subscribe(()=>this.stateChanges.next(void 0)),this._model}registerActions(e){this._actionsPortal,this._actionsPortal=e,this._componentRef?.instance._assignActions(e,!0)}removeActions(e){e===this._actionsPortal&&(this._actionsPortal=null,this._componentRef?.instance._assignActions(null,!0))}open(){this._opened||this.disabled||this._componentRef?.instance._isAnimating||(this.datepickerInput,this._focusedElementBeforeOpen=Mo(),this._openOverlay(),this._opened=!0,this.openedStream.emit())}close(){if(!this._opened||this._componentRef?.instance._isAnimating)return;let e=this.restoreFocus&&this._focusedElementBeforeOpen&&typeof this._focusedElementBeforeOpen.focus=="function",r=()=>{this._opened&&(this._opened=!1,this.closedStream.emit())};if(this._componentRef){let{instance:i,location:o}=this._componentRef;i._animationDone.pipe(Ft(1)).subscribe(()=>{let a=this._document.activeElement;e&&(!a||a===this._document.activeElement||o.nativeElement.contains(a))&&this._focusedElementBeforeOpen.focus(),this._focusedElementBeforeOpen=null,this._destroyOverlay()}),i._startExitAnimation()}e?setTimeout(r):r()}_applyPendingSelection(){this._componentRef?.instance?._applyPendingSelection()}_forwardContentValues(e){e.datepicker=this,e.color=this.color,e._dialogLabelId=this.datepickerInput.getOverlayLabelId(),e._assignActions(this._actionsPortal,!1)}_openOverlay(){this._destroyOverlay();let e=this.touchUi,r=new Rr(vD,this._viewContainerRef),i=this._overlayRef=Zo(this._injector,new Wo({positionStrategy:e?this._getDialogStrategy():this._getDropdownStrategy(),hasBackdrop:!0,backdropClass:[e?"cdk-overlay-dark-backdrop":"mat-overlay-transparent-backdrop",this._backdropHarnessClass],direction:this._dir||"ltr",scrollStrategy:e?su(this._injector):this._scrollStrategy(),panelClass:`mat-datepicker-${e?"dialog":"popup"}`,disableAnimations:this._animationsDisabled}));this._getCloseStream(i).subscribe(o=>{o&&o.preventDefault(),this.close()}),i.keydownEvents().subscribe(o=>{let a=o.keyCode;(a===38||a===40||a===37||a===39||a===33||a===34)&&o.preventDefault()}),this._componentRef=i.attach(r),this._forwardContentValues(this._componentRef.instance),e||it(()=>{i.updatePosition()},{injector:this._injector})}_destroyOverlay(){this._overlayRef&&(this._overlayRef.dispose(),this._overlayRef=this._componentRef=null)}_getDialogStrategy(){return lu(this._injector).centerHorizontally().centerVertically()}_getDropdownStrategy(){let e=Ko(this._injector,this.datepickerInput.getConnectedOverlayOrigin()).withTransformOriginOn(".mat-datepicker-content").withFlexibleDimensions(!1).withViewportMargin(8).withLockedPosition();return this._setConnectedPositions(e)}_setConnectedPositions(e){let r=this.xPosition==="end"?"end":"start",i=r==="start"?"end":"start",o=this.yPosition==="above"?"bottom":"top",a=o==="top"?"bottom":"top";return e.withPositions([{originX:r,originY:a,overlayX:r,overlayY:o},{originX:r,originY:o,overlayX:r,overlayY:a},{originX:i,originY:a,overlayX:i,overlayY:o},{originX:i,originY:o,overlayX:i,overlayY:a}])}_getCloseStream(e){let r=["ctrlKey","shiftKey","metaKey"];return Wr(e.backdropClick(),e.detachments(),e.keydownEvents().pipe(Ye(i=>i.keyCode===27&&!xn(i)||this.datepickerInput&&xn(i,"altKey")&&i.keyCode===38&&r.every(o=>!xn(i,o)))))}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,inputs:{calendarHeaderComponent:"calendarHeaderComponent",startAt:"startAt",startView:"startView",color:"color",touchUi:[2,"touchUi","touchUi",N],disabled:[2,"disabled","disabled",N],xPosition:"xPosition",yPosition:"yPosition",restoreFocus:[2,"restoreFocus","restoreFocus",N],dateClass:"dateClass",panelClass:"panelClass",opened:[2,"opened","opened",N]},outputs:{yearSelected:"yearSelected",monthSelected:"monthSelected",viewChanged:"viewChanged",openedStream:"opened",closedStream:"closed"},features:[Ae]})}return t})(),yD=(()=>{class t extends mD{static \u0275fac=(()=>{let e;return function(i){return(e||(e=gn(t)))(i||t)}})();static \u0275cmp=k({type:t,selectors:[["mat-datepicker"]],exportAs:["matDatepicker"],features:[ke([fD,{provide:mD,useExisting:t}]),nt],decls:0,vars:0,template:function(r,i){},encapsulation:2})}return t})(),Qo=class{target;targetElement;value=null;constructor(n,e){this.target=n,this.targetElement=e,this.value=this.target.value}},VR=(()=>{class t{_elementRef=u(V);_dateAdapter=u(_t,{optional:!0});_dateFormats=u(Ar,{optional:!0});_isInitialized=!1;get value(){return this._model?this._getValueFromModel(this._model.selection):this._pendingValue}set value(e){this._assignValueProgrammatically(e,!0)}_model;get disabled(){return!!this._disabled||this._parentDisabled()}set disabled(e){let r=e,i=this._elementRef.nativeElement;this._disabled!==r&&(this._disabled=r,this.stateChanges.next(void 0)),r&&this._isInitialized&&i.blur&&i.blur()}_disabled;dateChange=new C;dateInput=new C;stateChanges=new E;_onTouched=()=>{};_validatorOnChange=()=>{};_cvaOnChange=()=>{};_valueChangesSubscription=ue.EMPTY;_localeSubscription=ue.EMPTY;_pendingValue=null;_parseValidator=()=>this._lastValueValid?null:{matDatepickerParse:{text:this._elementRef.nativeElement.value}};_filterValidator=e=>{let r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value));return!r||this._matchesFilter(r)?null:{matDatepickerFilter:!0}};_minValidator=e=>{let r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value)),i=this._getMinDate();return!i||!r||this._dateAdapter.compareDate(i,r)<=0?null:{matDatepickerMin:{min:i,actual:r}}};_maxValidator=e=>{let r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e.value)),i=this._getMaxDate();return!i||!r||this._dateAdapter.compareDate(i,r)>=0?null:{matDatepickerMax:{max:i,actual:r}}};_getValidators(){return[this._parseValidator,this._minValidator,this._maxValidator,this._filterValidator]}_registerModel(e){this._model=e,this._valueChangesSubscription.unsubscribe(),this._pendingValue&&this._assignValue(this._pendingValue),this._valueChangesSubscription=this._model.selectionChanged.subscribe(r=>{if(this._shouldHandleChangeEvent(r)){let i=this._getValueFromModel(r.selection);this._lastValueValid=this._isValidValue(i),this._cvaOnChange(i),this._onTouched(),this._formatValue(i),this.dateInput.emit(new Qo(this,this._elementRef.nativeElement)),this.dateChange.emit(new Qo(this,this._elementRef.nativeElement))}})}_lastValueValid=!1;constructor(){this._localeSubscription=this._dateAdapter.localeChanges.subscribe(()=>{this._assignValueProgrammatically(this.value,!0)})}ngAfterViewInit(){this._isInitialized=!0}ngOnChanges(e){jR(e,this._dateAdapter)&&this.stateChanges.next(void 0)}ngOnDestroy(){this._valueChangesSubscription.unsubscribe(),this._localeSubscription.unsubscribe(),this.stateChanges.complete()}registerOnValidatorChange(e){this._validatorOnChange=e}validate(e){return this._validator?this._validator(e):null}writeValue(e){this._assignValueProgrammatically(e,e!==this.value)}registerOnChange(e){this._cvaOnChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_onKeydown(e){let r=["ctrlKey","shiftKey","metaKey"];xn(e,"altKey")&&e.keyCode===40&&r.every(o=>!xn(e,o))&&!this._elementRef.nativeElement.readOnly&&(this._openPopup(),e.preventDefault())}_onInput(e){let r=e.target.value,i=this._lastValueValid,o=this._dateAdapter.parse(r,this._dateFormats.parse.dateInput);this._lastValueValid=this._isValidValue(o),o=this._dateAdapter.getValidDateOrNull(o);let a=!this._dateAdapter.sameDate(o,this.value);!o||a?this._cvaOnChange(o):r&&!this.value&&this._cvaOnChange(o),!a&&i!==this._lastValueValid&&this._validatorOnChange(),a&&(this._assignValue(o),this.dateInput.emit(new Qo(this,this._elementRef.nativeElement)))}_onChange(){this.dateChange.emit(new Qo(this,this._elementRef.nativeElement))}_onBlur(){this.value&&this._formatValue(this.value),this._onTouched()}_formatValue(e){this._elementRef.nativeElement.value=e!=null?this._dateAdapter.format(e,this._dateFormats.display.dateInput):""}_assignValue(e){this._model?(this._assignValueToModel(e),this._pendingValue=null):this._pendingValue=e}_isValidValue(e){return!e||this._dateAdapter.isValid(e)}_parentDisabled(){return!1}_assignValueProgrammatically(e,r){e=this._dateAdapter.deserialize(e),this._lastValueValid=this._isValidValue(e),e=this._dateAdapter.getValidDateOrNull(e),this._assignValue(e),r&&this._formatValue(e)}_matchesFilter(e){let r=this._getDateFilter();return!r||r(e)}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,inputs:{value:"value",disabled:[2,"disabled","disabled",N]},outputs:{dateChange:"dateChange",dateInput:"dateInput"},features:[Ae]})}return t})();function jR(t,n){let e=Object.keys(t);for(let r of e){let{previousValue:i,currentValue:o}=t[r];if(n.isDateInstance(i)&&n.isDateInstance(o)){if(!n.sameDate(i,o))return!0}else return!0}return!1}var BR={provide:wn,useExisting:$e(()=>fu),multi:!0},zR={provide:rr,useExisting:$e(()=>fu),multi:!0},fu=(()=>{class t extends VR{_formField=u(Ps,{optional:!0});_closedSubscription=ue.EMPTY;_openedSubscription=ue.EMPTY;set matDatepicker(e){e&&(this._datepicker=e,this._ariaOwns.set(e.opened?e.id:null),this._closedSubscription=e.closedStream.subscribe(()=>{this._onTouched(),this._ariaOwns.set(null)}),this._openedSubscription=e.openedStream.subscribe(()=>{this._ariaOwns.set(e.id)}),this._registerModel(e.registerInput(this)))}_datepicker;_ariaOwns=A(null);get min(){return this._min}set min(e){let r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e));this._dateAdapter.sameDate(r,this._min)||(this._min=r,this._validatorOnChange())}_min=null;get max(){return this._max}set max(e){let r=this._dateAdapter.getValidDateOrNull(this._dateAdapter.deserialize(e));this._dateAdapter.sameDate(r,this._max)||(this._max=r,this._validatorOnChange())}_max=null;get dateFilter(){return this._dateFilter}set dateFilter(e){let r=this._matchesFilter(this.value);this._dateFilter=e,this._matchesFilter(this.value)!==r&&this._validatorOnChange()}_dateFilter;_validator=null;constructor(){super(),this._validator=ki.compose(super._getValidators())}getConnectedOverlayOrigin(){return this._formField?this._formField.getConnectedOverlayOrigin():this._elementRef}getOverlayLabelId(){return this._formField?this._formField.getLabelId():this._elementRef.nativeElement.getAttribute("aria-labelledby")}getThemePalette(){return this._formField?this._formField.color:void 0}getStartValue(){return this.value}ngOnDestroy(){super.ngOnDestroy(),this._closedSubscription.unsubscribe(),this._openedSubscription.unsubscribe()}_openPopup(){this._datepicker&&this._datepicker.open()}_getValueFromModel(e){return e}_assignValueToModel(e){this._model&&this._model.updateSelection(e,this)}_getMinDate(){return this._min}_getMaxDate(){return this._max}_getDateFilter(){return this._dateFilter}_shouldHandleChangeEvent(e){return e.source!==this}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["input","matDatepicker",""]],hostAttrs:[1,"mat-datepicker-input"],hostVars:6,hostBindings:function(r,i){r&1&&B("input",function(a){return i._onInput(a)})("change",function(){return i._onChange()})("blur",function(){return i._onBlur()})("keydown",function(a){return i._onKeydown(a)}),r&2&&(mt("disabled",i.disabled),de("aria-haspopup",i._datepicker?"dialog":null)("aria-owns",i._ariaOwns())("min",i.min?i._dateAdapter.toIso8601(i.min):null)("max",i.max?i._dateAdapter.toIso8601(i.max):null)("data-mat-calendar",i._datepicker?i._datepicker.id:null))},inputs:{matDatepicker:"matDatepicker",min:"min",max:"max",dateFilter:[0,"matDatepickerFilter","dateFilter"]},exportAs:["matDatepickerInput"],features:[ke([BR,zR,{provide:Zd,useExisting:t}]),nt]})}return t})(),HR=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["","matDatepickerToggleIcon",""]]})}return t})(),_g=(()=>{class t{_intl=u(ta);_changeDetectorRef=u(ve);_stateChanges=ue.EMPTY;datepicker;tabIndex=null;ariaLabel;get disabled(){return this._disabled===void 0&&this.datepicker?this.datepicker.disabled:!!this._disabled}set disabled(e){this._disabled=e}_disabled;disableRipple=!1;_customIcon;_button;constructor(){let e=u(new jt("tabindex"),{optional:!0}),r=Number(e);this.tabIndex=r||r===0?r:null}ngOnChanges(e){e.datepicker&&this._watchStateChanges()}ngOnDestroy(){this._stateChanges.unsubscribe()}ngAfterContentInit(){this._watchStateChanges()}_open(e){this.datepicker&&!this.disabled&&(this.datepicker.open(),e.stopPropagation())}_watchStateChanges(){let e=this.datepicker?this.datepicker.stateChanges:Y(),r=this.datepicker&&this.datepicker.datepickerInput?this.datepicker.datepickerInput.stateChanges:Y(),i=this.datepicker?Wr(this.datepicker.openedStream,this.datepicker.closedStream):Y();this._stateChanges.unsubscribe(),this._stateChanges=Wr(this._intl.changes,e,r,i).subscribe(()=>this._changeDetectorRef.markForCheck())}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["button"],r=[[["","matDatepickerToggleIcon",""]]],i=["[matDatepickerToggleIcon]"];function o(a,s){a&1&&(un(),v(0,"svg",2),Z(1,"path",3),y())}return k({type:t,selectors:[["mat-datepicker-toggle"]],contentQueries:function(s,l,c){if(s&1&&Ln(c,HR,5),s&2){let d;ae(d=se())&&(l._customIcon=d.first)}},viewQuery:function(s,l){if(s&1&&He(e,5),s&2){let c;ae(c=se())&&(l._button=c.first)}},hostAttrs:[1,"mat-datepicker-toggle"],hostVars:8,hostBindings:function(s,l){s&1&&B("click",function(d){return l._open(d)}),s&2&&(de("tabindex",null)("data-mat-calendar",l.datepicker?l.datepicker.id:null),U("mat-datepicker-toggle-active",l.datepicker&&l.datepicker.opened)("mat-accent",l.datepicker&&l.datepicker.color==="accent")("mat-warn",l.datepicker&&l.datepicker.color==="warn"))},inputs:{datepicker:[0,"for","datepicker"],tabIndex:"tabIndex",ariaLabel:[0,"aria-label","ariaLabel"],disabled:[2,"disabled","disabled",N],disableRipple:"disableRipple"},exportAs:["matDatepickerToggle"],features:[Ae],ngContentSelectors:i,decls:4,vars:7,consts:[["button",""],["matIconButton","","type","button",3,"tabIndex","disabled","disableRipple"],["viewBox","0 0 24 24","width","24px","height","24px","fill","currentColor","focusable","false","aria-hidden","true",1,"mat-datepicker-toggle-default-icon"],["d","M19 3h-1V1h-2v2H8V1H6v2H5c-1.11 0-1.99.9-1.99 2L3 19c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm0 16H5V8h14v11zM7 10h5v5H7z"]],template:function(s,l){s&1&&(Oe(r),v(0,"button",1,0),xe(2,o,2,0,"svg",2),ce(3),y()),s&2&&(z("tabIndex",l.disabled?-1:l.tabIndex)("disabled",l.disabled)("disableRipple",l.disableRipple),de("aria-haspopup",l.datepicker?"dialog":null)("aria-label",l.ariaLabel||l._intl.openCalendarLabel)("aria-expanded",l.datepicker?l.datepicker.opened:null),_(2),we(l._customIcon?-1:2))},dependencies:[Md],styles:[`.mat-datepicker-toggle {
  pointer-events: auto;
  color: var(--%NS%mat-datepicker-toggle-icon-color, var(--%NS%mat-sys-on-surface-variant));
}
.mat-datepicker-toggle button {
  color: inherit;
}

.mat-datepicker-toggle-active {
  color: var(--%NS%mat-datepicker-toggle-active-state-icon-color, var(--%NS%mat-sys-primary));
}

@media (forced-colors: active) {
  .mat-datepicker-toggle-default-icon {
    color: CanvasText;
  }
}
`],encapsulation:2})})()}return t})();var _D=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({providers:[ta],imports:[Bn,cg,Ih,eu,vD,_g,bD,Fe,Ls]})}return t})();var xD=(()=>{class t{_animationsDisabled=Be();state="unchecked";disabled=!1;appearance="full";static \u0275fac=function(r){return new(r||t)};static \u0275cmp=k({type:t,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(r,i){r&2&&U("mat-pseudo-checkbox-indeterminate",i.state==="indeterminate")("mat-pseudo-checkbox-checked",i.state==="checked")("mat-pseudo-checkbox-disabled",i.disabled)("mat-pseudo-checkbox-minimal",i.appearance==="minimal")("mat-pseudo-checkbox-full",i.appearance==="full")("_mat-animation-noopable",i._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(r,i){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--%NS%mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--%NS%mat-pseudo-checkbox-full-unselected-icon-color, var(--%NS%mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--%NS%mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--%NS%mat-pseudo-checkbox-full-selected-icon-color, var(--%NS%mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--%NS%mat-pseudo-checkbox-full-selected-checkmark-color, var(--%NS%mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--%NS%mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--%NS%mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2})}return t})();var $R=/^\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|(?:(?:\+|-)\d{2}:\d{2}))?)?$/,GR=/^(\d?\d)[:.](\d?\d)(?:[:.](\d?\d))?\s*(AM|PM)?$/i;function xg(t,n){let e=Array(t);for(let r=0;r<t;r++)e[r]=n(r);return e}var WR=(()=>{class t extends _t{_matDateLocale=u(tg,{optional:!0});constructor(){super();let e=u(tg,{optional:!0});e!==void 0&&(this._matDateLocale=e),super.setLocale(this._matDateLocale)}getYear(e){return e.getFullYear()}getMonth(e){return e.getMonth()}getDate(e){return e.getDate()}getDayOfWeek(e){return e.getDay()}getMonthNames(e){let r=new Intl.DateTimeFormat(this.locale,{month:e,timeZone:"utc"});return xg(12,i=>this._format(r,new Date(2017,i,1)))}getDateNames(){let e=new Intl.DateTimeFormat(this.locale,{day:"numeric",timeZone:"utc"});return xg(31,r=>this._format(e,new Date(2017,0,r+1)))}getDayOfWeekNames(e){let r=new Intl.DateTimeFormat(this.locale,{weekday:e,timeZone:"utc"});return xg(7,i=>this._format(r,new Date(2017,0,i+1)))}getYearName(e){let r=new Intl.DateTimeFormat(this.locale,{year:"numeric",timeZone:"utc"});return this._format(r,e)}getFirstDayOfWeek(){if(typeof Intl<"u"&&Intl.Locale){let e=new Intl.Locale(this.locale),r=(e.getWeekInfo?.()||e.weekInfo)?.firstDay??0;return r===7?0:r}return 0}getNumDaysInMonth(e){return this.getDate(this._createDateWithOverflow(this.getYear(e),this.getMonth(e)+1,0))}clone(e){return new Date(e.getTime())}createDate(e,r,i){let o=this._createDateWithOverflow(e,r,i);return o.getMonth()!=r,o}today(){return new Date}parse(e,r){return typeof e=="number"?new Date(e):e?new Date(Date.parse(e)):null}format(e,r){if(!this.isValid(e))throw Error("NativeDateAdapter: Cannot format invalid date.");let i=new Intl.DateTimeFormat(this.locale,R(g({},r),{timeZone:"utc"}));return this._format(i,e)}addCalendarYears(e,r){return this.addCalendarMonths(e,r*12)}addCalendarMonths(e,r){let i=this._createDateWithOverflow(this.getYear(e),this.getMonth(e)+r,this.getDate(e));return this.getMonth(i)!=((this.getMonth(e)+r)%12+12)%12&&(i=this._createDateWithOverflow(this.getYear(i),this.getMonth(i),0)),i}addCalendarDays(e,r){return this._createDateWithOverflow(this.getYear(e),this.getMonth(e),this.getDate(e)+r)}toIso8601(e){return[e.getUTCFullYear(),this._2digit(e.getUTCMonth()+1),this._2digit(e.getUTCDate())].join("-")}deserialize(e){if(typeof e=="string"){if(!e)return null;if($R.test(e)){let r=new Date(e);if(this.isValid(r))return r}}return super.deserialize(e)}isDateInstance(e){return e instanceof Date}isValid(e){return!isNaN(e.getTime())}invalid(){return new Date(NaN)}setTime(e,r,i,o){let a=this.clone(e);return a.setHours(r,i,o,0),a}getHours(e){return e.getHours()}getMinutes(e){return e.getMinutes()}getSeconds(e){return e.getSeconds()}parseTime(e,r){if(typeof e!="string")return e instanceof Date?new Date(e.getTime()):null;let i=e.trim();if(i.length===0)return null;let o=this._parseTimeString(i);if(o===null){let a=i.replace(/[^0-9:(AM|PM)]/gi,"").trim();a.length>0&&(o=this._parseTimeString(a))}return o||this.invalid()}addSeconds(e,r){return new Date(e.getTime()+r*1e3)}_createDateWithOverflow(e,r,i){let o=new Date;return o.setFullYear(e,r,i),o.setHours(0,0,0,0),o}_2digit(e){return("00"+e).slice(-2)}_format(e,r){let i=new Date;return i.setUTCFullYear(r.getFullYear(),r.getMonth(),r.getDate()),i.setUTCHours(r.getHours(),r.getMinutes(),r.getSeconds(),r.getMilliseconds()),e.format(i)}_parseTimeString(e){let r=e.toUpperCase().match(GR);if(r){let i=parseInt(r[1]),o=parseInt(r[2]),a=r[3]==null?void 0:parseInt(r[3]),s=r[4];if(i===12?i=s==="AM"?0:i:s==="PM"&&(i+=12),wg(i,0,23)&&wg(o,0,59)&&(a==null||wg(a,0,59)))return this.setTime(this.today(),i,o,a||0)}return null}static \u0275fac=function(r){return new(r||t)};static \u0275prov=S({token:t,factory:t.\u0275fac,autoProvided:!1})}return t})();function wg(t,n,e){return!isNaN(t)&&t>=n&&t<=e}var qR={parse:{dateInput:null,timeInput:null},display:{dateInput:{year:"numeric",month:"numeric",day:"numeric"},timeInput:{hour:"numeric",minute:"numeric"},monthYearLabel:{year:"numeric",month:"short"},dateA11yLabel:{year:"numeric",month:"long",day:"numeric"},monthYearA11yLabel:{year:"numeric",month:"long"},timeOptionLabel:{hour:"numeric",minute:"numeric"}}};function wD(t=qR){return[{provide:_t,useClass:WR},{provide:Ar,useValue:t}]}var pu=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-elee-date-picker"]],features:[ke([wD()])],decls:12,vars:2,consts:[["picker",""],["appearance","outlined",2,"margin-top","16px"],[1,"delivery-date-container",2,"padding-top","8px"],["appearance","fill",2,"width","100%"],["matInput","",3,"matDatepicker"],["matIconSuffix","",3,"for"]],template:function(e,r){if(e&1&&(v(0,"mat-card",1)(1,"mat-card-content")(2,"div",2)(3,"mat-form-field",3)(4,"mat-label"),M(5,"Choose delivery date"),y(),Z(6,"input",4),v(7,"mat-hint"),M(8,"MM/DD/YYYY"),y(),Z(9,"mat-datepicker-toggle",5)(10,"mat-datepicker",null,0),y()()()()),e&2){let i=Zt(11);_(6),z("matDatepicker",i),_(3),z("for",i)}},dependencies:[an,At,Rt,or,Tr,ir,Fs,Xh,Ho,zo,_D,yD,fu,_g],encapsulation:2})};var YR=new b("mat-slide-toggle-default-options",{providedIn:"root",factory:()=>({disableToggleValue:!1,hideIcon:!1,disabledInteractive:!1})}),hu=class{source;checked;constructor(n,e){this.source=n,this.checked=e}},Cg=(()=>{class t{_elementRef=u(V);_focusMonitor=u(en);_changeDetectorRef=u(ve);defaults=u(YR);_onChange=e=>{};_onTouched=()=>{};_validatorOnChange=()=>{};_uniqueId;_checked=!1;_createChangeEvent(e){return new hu(this,e)}_labelId;get buttonId(){return`${this.id||this._uniqueId}-button`}_switchElement;focus(){this._switchElement.nativeElement.focus()}_noopAnimations=Be();_focused=!1;name=null;id;labelPosition="after";ariaLabel=null;ariaLabelledby=null;ariaDescribedby;required=!1;color;disabled=!1;fullWidth=!1;disableRipple=!1;tabIndex=0;get checked(){return this._checked}set checked(e){this._checked=e,this._changeDetectorRef.markForCheck()}hideIcon;disabledInteractive;change=new C;toggleChange=new C;get inputId(){return`${this.id||this._uniqueId}-input`}constructor(){u(Me).load(nn);let e=u(new jt("tabindex"),{optional:!0}),r=this.defaults;this.tabIndex=e==null?0:parseInt(e)||0,this.color=r.color||"accent",this.id=this._uniqueId=u(je).getId("mat-mdc-slide-toggle-"),this.hideIcon=r.hideIcon??!1,this.disabledInteractive=r.disabledInteractive??!1,this._labelId=this._uniqueId+"-label"}ngAfterContentInit(){this._focusMonitor.monitor(this._elementRef,!0).subscribe(e=>{e==="keyboard"||e==="program"?(this._focused=!0,this._changeDetectorRef.markForCheck()):e||Promise.resolve().then(()=>{this._focused=!1,this._onTouched(),this._changeDetectorRef.markForCheck()})})}ngOnChanges(e){e.required&&this._validatorOnChange()}ngOnDestroy(){this._focusMonitor.stopMonitoring(this._elementRef)}writeValue(e){this.checked=!!e}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorOnChange=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck()}toggle(){this.checked=!this.checked,this._onChange(this.checked)}_emitChangeEvent(){this._onChange(this.checked),this.change.emit(this._createChangeEvent(this.checked))}_handleClick(){this.disabled||(this.toggleChange.emit(),this.defaults.disableToggleValue||(this.checked=!this.checked,this._onChange(this.checked),this.change.emit(new hu(this,this.checked))))}_getAriaLabelledBy(){return this.ariaLabelledby?this.ariaLabelledby:this.ariaLabel?null:this._labelId}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["switch"],r=["*"];function i(o,a){o&1&&(v(0,"span",11),un(),v(1,"svg",13),Z(2,"path",14),y(),v(3,"svg",15),Z(4,"path",16),y()())}return k({type:t,selectors:[["mat-slide-toggle"]],viewQuery:function(a,s){if(a&1&&He(e,5),a&2){let l;ae(l=se())&&(s._switchElement=l.first)}},hostAttrs:[1,"mat-mdc-slide-toggle"],hostVars:15,hostBindings:function(a,s){a&2&&(mt("id",s.id),de("tabindex",null)("aria-label",null)("name",null)("aria-labelledby",null),Vt(s.color?"mat-"+s.color:""),U("mat-mdc-slide-toggle-focused",s._focused)("mat-mdc-slide-toggle-checked",s.checked)("mat-slide-toggle-full-width",s.fullWidth)("_mat-animation-noopable",s._noopAnimations))},inputs:{name:"name",id:"id",labelPosition:"labelPosition",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],required:[2,"required","required",N],color:"color",disabled:[2,"disabled","disabled",N],fullWidth:[2,"fullWidth","fullWidth",N],disableRipple:[2,"disableRipple","disableRipple",N],tabIndex:[2,"tabIndex","tabIndex",o=>o==null?0:er(o)],checked:[2,"checked","checked",N],hideIcon:[2,"hideIcon","hideIcon",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},outputs:{change:"change",toggleChange:"toggleChange"},exportAs:["matSlideToggle"],features:[ke([{provide:wn,useExisting:$e(()=>t),multi:!0},{provide:rr,useExisting:t,multi:!0}]),Ae],ngContentSelectors:r,decls:14,vars:27,consts:[["switch",""],["mat-internal-form-field","",3,"labelPosition"],["role","switch","type","button",1,"mdc-switch",3,"click","tabIndex","disabled"],[1,"mat-mdc-slide-toggle-touch-target"],[1,"mdc-switch__track"],[1,"mdc-switch__handle-track"],[1,"mdc-switch__handle"],[1,"mdc-switch__shadow"],[1,"mdc-elevation-overlay"],[1,"mdc-switch__ripple"],["mat-ripple","",1,"mat-mdc-slide-toggle-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-switch__icons"],[1,"mdc-label",3,"click","for"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--on"],["d","M19.69,5.23L8.96,15.96l-4.23-4.23L2.96,13.5l6,6L21.46,7L19.69,5.23z"],["viewBox","0 0 24 24","aria-hidden","true",1,"mdc-switch__icon","mdc-switch__icon--off"],["d","M20 13H4v-2h16v2z"]],template:function(a,s){if(a&1&&(Oe(),v(0,"div",1)(1,"button",2,0),B("click",function(){return s._handleClick()}),Z(3,"div",3)(4,"span",4),v(5,"span",5)(6,"span",6)(7,"span",7),Z(8,"span",8),y(),v(9,"span",9),Z(10,"span",10),y(),xe(11,i,5,0,"span",11),y()()(),v(12,"label",12),B("click",function(c){return c.stopPropagation()}),ce(13),y()()),a&2){let l=Zt(2);z("labelPosition",s.labelPosition),_(),U("mdc-switch--selected",s.checked)("mdc-switch--unselected",!s.checked)("mdc-switch--checked",s.checked)("mdc-switch--disabled",s.disabled)("mat-mdc-slide-toggle-disabled-interactive",s.disabledInteractive),z("tabIndex",s.disabled&&!s.disabledInteractive?-1:s.tabIndex)("disabled",s.disabled&&!s.disabledInteractive),de("id",s.buttonId)("name",s.name)("aria-label",s.ariaLabel)("aria-labelledby",s._getAriaLabelledBy())("aria-describedby",s.ariaDescribedby)("aria-required",s.required||null)("aria-checked",s.checked)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),_(9),z("matRippleTrigger",l)("matRippleDisabled",s.disableRipple||s.disabled)("matRippleCentered",!0),_(),we(s.hideIcon?-1:11),_(),z("for",s.buttonId),de("id",s._labelId)}},dependencies:[Mr,jo],styles:[`.mdc-switch {
  align-items: center;
  background: none;
  border: none;
  cursor: pointer;
  display: inline-flex;
  flex-shrink: 0;
  margin: 0;
  outline: none;
  overflow: visible;
  padding: 0;
  position: relative;
  width: var(--%NS%mat-slide-toggle-track-width, 52px);
}
.mdc-switch.mdc-switch--disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-switch.mat-mdc-slide-toggle-disabled-interactive {
  pointer-events: auto;
}

.mdc-switch__track {
  overflow: hidden;
  position: relative;
  width: 100%;
  height: var(--%NS%mat-slide-toggle-track-height, 32px);
  border-radius: var(--%NS%mat-slide-toggle-track-shape, var(--%NS%mat-sys-corner-full));
}
.mdc-switch--disabled.mdc-switch .mdc-switch__track {
  opacity: var(--%NS%mat-slide-toggle-disabled-track-opacity, 0.12);
}
.mdc-switch__track::before, .mdc-switch__track::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  height: 100%;
  left: 0;
  position: absolute;
  width: 100%;
  border-width: var(--%NS%mat-slide-toggle-track-outline-width, 2px);
  border-color: var(--%NS%mat-slide-toggle-track-outline-color, var(--%NS%mat-sys-outline));
}
.mdc-switch--selected .mdc-switch__track::before, .mdc-switch--selected .mdc-switch__track::after {
  border-width: var(--%NS%mat-slide-toggle-selected-track-outline-width, 2px);
  border-color: var(--%NS%mat-slide-toggle-selected-track-outline-color, transparent);
}
.mdc-switch--disabled .mdc-switch__track::before, .mdc-switch--disabled .mdc-switch__track::after {
  border-width: var(--%NS%mat-slide-toggle-disabled-unselected-track-outline-width, 2px);
  border-color: var(--%NS%mat-slide-toggle-disabled-unselected-track-outline-color, var(--%NS%mat-sys-on-surface));
}
@media (forced-colors: active) {
  .mdc-switch__track {
    border-color: currentColor;
  }
}
.mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: translateX(0);
  background: var(--%NS%mat-slide-toggle-unselected-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__track::before {
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.6, 1);
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch--selected .mdc-switch__track::before {
  transform: translateX(-100%);
}
.mdc-switch--selected .mdc-switch__track::before {
  opacity: var(--%NS%mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--%NS%mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::before {
  opacity: var(--%NS%mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--%NS%mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-unselected-hover-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-unselected-focus-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch:enabled:active .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-unselected-pressed-track-color, var(--%NS%mat-sys-surface-variant));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:hover:not(:focus):not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:focus:not(:active) .mdc-switch__track::before, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:active .mdc-switch__track::before, .mdc-switch.mdc-switch--disabled .mdc-switch__track::before {
  background: var(--%NS%mat-slide-toggle-disabled-unselected-track-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch__track::after {
  transform: translateX(-100%);
  background: var(--%NS%mat-slide-toggle-selected-track-color, var(--%NS%mat-sys-primary));
}
[dir=rtl] .mdc-switch__track::after {
  transform: translateX(100%);
}
.mdc-switch--selected .mdc-switch__track::after {
  transform: translateX(0);
}
.mdc-switch--selected .mdc-switch__track::after {
  opacity: var(--%NS%mat-slide-toggle-visible-track-opacity, 1);
  transition: var(--%NS%mat-slide-toggle-visible-track-transition, opacity 75ms);
}
.mdc-switch--unselected .mdc-switch__track::after {
  opacity: var(--%NS%mat-slide-toggle-hidden-track-opacity, 0);
  transition: var(--%NS%mat-slide-toggle-hidden-track-transition, opacity 75ms);
}
.mdc-switch:enabled:hover:not(:focus):not(:active) .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-selected-hover-track-color, var(--%NS%mat-sys-primary));
}
.mdc-switch:enabled:focus:not(:active) .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-selected-focus-track-color, var(--%NS%mat-sys-primary));
}
.mdc-switch:enabled:active .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-selected-pressed-track-color, var(--%NS%mat-sys-primary));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:hover:not(:focus):not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:focus:not(:active) .mdc-switch__track::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:active .mdc-switch__track::after, .mdc-switch.mdc-switch--disabled .mdc-switch__track::after {
  background: var(--%NS%mat-slide-toggle-disabled-selected-track-color, var(--%NS%mat-sys-on-surface));
}

.mdc-switch__handle-track {
  height: 100%;
  pointer-events: none;
  position: absolute;
  top: 0;
  transition: transform 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  left: 0;
  right: auto;
  transform: translateX(0);
  width: calc(100% - var(--%NS%mat-slide-toggle-handle-width));
}
[dir=rtl] .mdc-switch__handle-track {
  left: auto;
  right: 0;
}
.mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(100%);
}
[dir=rtl] .mdc-switch--selected .mdc-switch__handle-track {
  transform: translateX(-100%);
}

.mdc-switch__handle {
  display: flex;
  pointer-events: auto;
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  left: 0;
  right: auto;
  transition: width 75ms cubic-bezier(0.4, 0, 0.2, 1), height 75ms cubic-bezier(0.4, 0, 0.2, 1), margin 75ms cubic-bezier(0.4, 0, 0.2, 1);
  width: var(--%NS%mat-slide-toggle-handle-width);
  height: var(--%NS%mat-slide-toggle-handle-height);
  border-radius: var(--%NS%mat-slide-toggle-handle-shape, var(--%NS%mat-sys-corner-full));
}
[dir=rtl] .mdc-switch__handle {
  left: auto;
  right: 0;
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle {
  width: var(--%NS%mat-slide-toggle-unselected-handle-size, 16px);
  height: var(--%NS%mat-slide-toggle-unselected-handle-size, 16px);
  margin: var(--%NS%mat-slide-toggle-unselected-handle-horizontal-margin, 0 8px);
}
.mat-mdc-slide-toggle .mdc-switch--unselected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--%NS%mat-slide-toggle-unselected-with-icon-handle-horizontal-margin, 0 4px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle {
  width: var(--%NS%mat-slide-toggle-selected-handle-size, 24px);
  height: var(--%NS%mat-slide-toggle-selected-handle-size, 24px);
  margin: var(--%NS%mat-slide-toggle-selected-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch--selected .mdc-switch__handle:has(.mdc-switch__icons) {
  margin: var(--%NS%mat-slide-toggle-selected-with-icon-handle-horizontal-margin, 0 24px);
}
.mat-mdc-slide-toggle .mdc-switch__handle:has(.mdc-switch__icons) {
  width: var(--%NS%mat-slide-toggle-with-icon-handle-size, 24px);
  height: var(--%NS%mat-slide-toggle-with-icon-handle-size, 24px);
}
.mat-mdc-slide-toggle .mdc-switch:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  width: var(--%NS%mat-slide-toggle-pressed-handle-size, 28px);
  height: var(--%NS%mat-slide-toggle-pressed-handle-size, 28px);
}
.mat-mdc-slide-toggle .mdc-switch--%NS%selected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--%NS%mat-slide-toggle-selected-pressed-handle-horizontal-margin, 0 22px);
}
.mat-mdc-slide-toggle .mdc-switch--%NS%unselected:active:not(.mdc-switch--disabled) .mdc-switch__handle {
  margin: var(--%NS%mat-slide-toggle-unselected-pressed-handle-horizontal-margin, 0 2px);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__handle::after {
  opacity: var(--%NS%mat-slide-toggle-disabled-selected-handle-opacity, 1);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__handle::after {
  opacity: var(--%NS%mat-slide-toggle-disabled-unselected-handle-opacity, 0.38);
}
.mdc-switch__handle::before, .mdc-switch__handle::after {
  border: 1px solid transparent;
  border-radius: inherit;
  box-sizing: border-box;
  content: "";
  width: 100%;
  height: 100%;
  left: 0;
  position: absolute;
  top: 0;
  transition: background-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1), border-color 75ms 0ms cubic-bezier(0.4, 0, 0.2, 1);
  z-index: -1;
}
@media (forced-colors: active) {
  .mdc-switch__handle::before, .mdc-switch__handle::after {
    border-color: currentColor;
  }
}
.mdc-switch--%NS%selected:enabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-handle-color, var(--%NS%mat-sys-on-primary));
}
.mdc-switch--%NS%selected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-hover-handle-color, var(--%NS%mat-sys-primary-container));
}
.mdc-switch--%NS%selected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-focus-handle-color, var(--%NS%mat-sys-primary-container));
}
.mdc-switch--%NS%selected:enabled:active .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-selected-pressed-handle-color, var(--%NS%mat-sys-primary-container));
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--%NS%selected:hover:not(:focus):not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--%NS%selected:focus:not(:active) .mdc-switch__handle::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--disabled.mdc-switch--%NS%selected:active .mdc-switch__handle::after, .mdc-switch--selected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-disabled-selected-handle-color, var(--%NS%mat-sys-surface));
}
.mdc-switch--%NS%unselected:enabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-handle-color, var(--%NS%mat-sys-outline));
}
.mdc-switch--%NS%unselected:enabled:hover:not(:focus):not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-hover-handle-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-switch--%NS%unselected:enabled:focus:not(:active) .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-focus-handle-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-switch--%NS%unselected:enabled:active .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-unselected-pressed-handle-color, var(--%NS%mat-sys-on-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__handle::after {
  background: var(--%NS%mat-slide-toggle-disabled-unselected-handle-color, var(--%NS%mat-sys-on-surface));
}
.mdc-switch__handle::before {
  background: var(--%NS%mat-slide-toggle-handle-surface-color);
}

.mdc-switch__shadow {
  border-radius: inherit;
  bottom: 0;
  left: 0;
  position: absolute;
  right: 0;
  top: 0;
}
.mdc-switch:enabled .mdc-switch__shadow {
  box-shadow: var(--%NS%mat-slide-toggle-handle-elevation-shadow);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:hover:not(:focus):not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:focus:not(:active) .mdc-switch__shadow, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:active .mdc-switch__shadow, .mdc-switch.mdc-switch--disabled .mdc-switch__shadow {
  box-shadow: var(--%NS%mat-slide-toggle-disabled-handle-elevation-shadow);
}

.mdc-switch__ripple {
  left: 50%;
  position: absolute;
  top: 50%;
  transform: translate(-50%, -50%);
  z-index: -1;
  width: var(--%NS%mat-slide-toggle-state-layer-size, 40px);
  height: var(--%NS%mat-slide-toggle-state-layer-size, 40px);
}
.mdc-switch__ripple::after {
  content: "";
  opacity: 0;
}
.mdc-switch--disabled .mdc-switch__ripple::after {
  display: none;
}
.mat-mdc-slide-toggle-disabled-interactive .mdc-switch__ripple::after {
  display: block;
}
.mdc-switch:hover .mdc-switch__ripple::after {
  transition: 75ms opacity cubic-bezier(0, 0, 0.2, 1);
}
.mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:enabled:focus .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:enabled:active .mdc-switch__ripple::after, .mat-mdc-slide-toggle-disabled-interactive.mdc-switch--%NS%disabled:enabled:hover:not(:focus) .mdc-switch__ripple::after, .mdc-switch--%NS%unselected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-unselected-hover-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-slide-toggle-unselected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mdc-switch--%NS%unselected:enabled:focus .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-unselected-focus-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-slide-toggle-unselected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mdc-switch--%NS%unselected:enabled:active .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-unselected-pressed-state-layer-color, var(--%NS%mat-sys-on-surface));
  opacity: var(--%NS%mat-slide-toggle-unselected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}
.mdc-switch--%NS%selected:enabled:hover:not(:focus) .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-selected-hover-state-layer-color, var(--%NS%mat-sys-primary));
  opacity: var(--%NS%mat-slide-toggle-selected-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mdc-switch--%NS%selected:enabled:focus .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-selected-focus-state-layer-color, var(--%NS%mat-sys-primary));
  opacity: var(--%NS%mat-slide-toggle-selected-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
.mdc-switch--%NS%selected:enabled:active .mdc-switch__ripple::after {
  background: var(--%NS%mat-slide-toggle-selected-pressed-state-layer-color, var(--%NS%mat-sys-primary));
  opacity: var(--%NS%mat-slide-toggle-selected-pressed-state-layer-opacity, var(--%NS%mat-sys-pressed-state-layer-opacity));
  transition: opacity 75ms linear;
}

.mdc-switch__icons {
  position: relative;
  height: 100%;
  width: 100%;
  z-index: 1;
  transform: translateZ(0);
}
.mdc-switch--disabled.mdc-switch--unselected .mdc-switch__icons {
  opacity: var(--%NS%mat-slide-toggle-disabled-unselected-icon-opacity, 0.38);
}
.mdc-switch--disabled.mdc-switch--selected .mdc-switch__icons {
  opacity: var(--%NS%mat-slide-toggle-disabled-selected-icon-opacity, 0.38);
}

.mdc-switch__icon {
  bottom: 0;
  left: 0;
  margin: auto;
  position: absolute;
  right: 0;
  top: 0;
  opacity: 0;
  transition: opacity 30ms 0ms cubic-bezier(0.4, 0, 1, 1);
}
.mdc-switch--unselected .mdc-switch__icon {
  width: var(--%NS%mat-slide-toggle-unselected-icon-size, 16px);
  height: var(--%NS%mat-slide-toggle-unselected-icon-size, 16px);
  fill: var(--%NS%mat-slide-toggle-unselected-icon-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch--unselected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--%NS%mat-slide-toggle-disabled-unselected-icon-color, var(--%NS%mat-sys-surface-variant));
}
.mdc-switch--selected .mdc-switch__icon {
  width: var(--%NS%mat-slide-toggle-selected-icon-size, 16px);
  height: var(--%NS%mat-slide-toggle-selected-icon-size, 16px);
  fill: var(--%NS%mat-slide-toggle-selected-icon-color, var(--%NS%mat-sys-on-primary-container));
}
.mdc-switch--selected.mdc-switch--disabled .mdc-switch__icon {
  fill: var(--%NS%mat-slide-toggle-disabled-selected-icon-color, var(--%NS%mat-sys-on-surface));
}

.mdc-switch--selected .mdc-switch__icon--on,
.mdc-switch--unselected .mdc-switch__icon--off {
  opacity: 1;
  transition: opacity 45ms 30ms cubic-bezier(0, 0, 0.2, 1);
}

.mat-mdc-slide-toggle {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  -webkit-tap-highlight-color: transparent;
  outline: 0;
}
.mat-mdc-slide-toggle .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple,
.mat-mdc-slide-toggle .mdc-switch__ripple::after {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-slide-toggle .mat-mdc-slide-toggle-ripple:not(:empty),
.mat-mdc-slide-toggle .mdc-switch__ripple::after:not(:empty) {
  transform: translateZ(0);
}
.mat-mdc-slide-toggle.mat-mdc-slide-toggle-focused .mat-focus-indicator::before {
  content: "";
}
.mat-mdc-slide-toggle .mat-internal-form-field {
  color: var(--%NS%mat-slide-toggle-label-text-color, var(--%NS%mat-sys-on-surface));
  font-family: var(--%NS%mat-slide-toggle-label-text-font, var(--%NS%mat-sys-body-medium-font));
  line-height: var(--%NS%mat-slide-toggle-label-text-line-height, var(--%NS%mat-sys-body-medium-line-height));
  font-size: var(--%NS%mat-slide-toggle-label-text-size, var(--%NS%mat-sys-body-medium-size));
  letter-spacing: var(--%NS%mat-slide-toggle-label-text-tracking, var(--%NS%mat-sys-body-medium-tracking));
  font-weight: var(--%NS%mat-slide-toggle-label-text-weight, var(--%NS%mat-sys-body-medium-weight));
}
.mat-mdc-slide-toggle .mat-ripple-element {
  opacity: 0.12;
}
.mat-mdc-slide-toggle .mat-focus-indicator::before {
  border-radius: 50%;
}
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle-track,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__icon,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__handle::after,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::before,
.mat-mdc-slide-toggle._mat-animation-noopable .mdc-switch__track::after {
  transition: none;
}
.mat-mdc-slide-toggle .mdc-switch:enabled + .mdc-label {
  cursor: pointer;
}
.mat-mdc-slide-toggle .mdc-switch--disabled + label {
  color: var(--%NS%mat-slide-toggle-disabled-label-text-color, var(--%NS%mat-sys-on-surface));
}
.mat-mdc-slide-toggle label:empty {
  display: none;
}

.mat-slide-toggle-full-width {
  width: 100%;
}
.mat-slide-toggle-full-width .mat-internal-form-field {
  width: 100%;
  justify-content: space-between;
}
.mat-slide-toggle-full-width .mat-internal-form-field label {
  margin: 0;
  flex-grow: 1;
  text-align: end;
}
.mat-slide-toggle-full-width .mdc-form-field--align-end label {
  text-align: start;
}

.mat-mdc-slide-toggle-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--%NS%mat-slide-toggle-touch-target-size, 48px);
  width: 100%;
  transform: translate(-50%, -50%);
  display: var(--%NS%mat-slide-toggle-touch-target-display, block);
}
[dir=rtl] .mat-mdc-slide-toggle-touch-target {
  left: auto;
  right: 50%;
  transform: translate(50%, -50%);
}
`],encapsulation:2})})()}return t})(),CD=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Cg,Fe]})}return t})();var gu=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-crust-selection"]],decls:11,vars:0,consts:[["appearance","outlined"]],template:function(e,r){e&1&&(v(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),M(3," Choose Your Pizza Crust "),y()(),v(4,"mat-card-content")(5,"p")(6,"mat-slide-toggle"),M(7," Pan Crust "),y()(),v(8,"p")(9,"mat-slide-toggle"),M(10," Hand Tossed "),y()()()())},dependencies:[an,At,Rt,on,rn,CD,Cg],encapsulation:2})};var DD=new b("MAT_BUTTON_TOGGLE_DEFAULT_OPTIONS",{providedIn:"root",factory:()=>({hideSingleSelectionIndicator:!1,hideMultipleSelectionIndicator:!1,disabledInteractive:!1})}),SD=new b("MatButtonToggleGroup"),ZR={provide:wn,useExisting:$e(()=>Dg),multi:!0},bu=class{source;value;constructor(n,e){this.source=n,this.value=e}},Dg=(()=>{class t{_changeDetector=u(ve);_dir=u(Tt,{optional:!0});_multiple=!1;_disabled=!1;_disabledInteractive=!1;_selectionModel;_rawValue;_controlValueAccessorChangeFn=()=>{};_onTouched=()=>{};_buttonToggles;appearance;get name(){return this._name}set name(e){this._name=e,this._markButtonsForCheck()}_name=u(je).getId("mat-button-toggle-group-");vertical=!1;get value(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e.map(r=>r.value):e[0]?e[0].value:void 0}set value(e){this._setSelectionByValue(e),this.valueChange.emit(this.value)}valueChange=new C;get selected(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e:e[0]||null}get multiple(){return this._multiple}set multiple(e){this._multiple=e,this._markButtonsForCheck()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markButtonsForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markButtonsForCheck()}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}change=new C;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._markButtonsForCheck()}_hideSingleSelectionIndicator;get hideMultipleSelectionIndicator(){return this._hideMultipleSelectionIndicator}set hideMultipleSelectionIndicator(e){this._hideMultipleSelectionIndicator=e,this._markButtonsForCheck()}_hideMultipleSelectionIndicator;constructor(){let e=u(DD,{optional:!0});this.appearance=e&&e.appearance?e.appearance:"standard",this._hideSingleSelectionIndicator=e?.hideSingleSelectionIndicator??!1,this._hideMultipleSelectionIndicator=e?.hideMultipleSelectionIndicator??!1}ngOnInit(){this._selectionModel=new Rs(this.multiple,void 0,!1)}ngAfterContentInit(){this._selectionModel.select(...this._buttonToggles.filter(e=>e.checked)),this.multiple||this._initializeTabIndex()}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_keydown(e){if(this.multiple||this.disabled||xn(e))return;let i=e.target.id,o=this._buttonToggles.toArray().findIndex(s=>s.buttonId===i),a=null;switch(e.keyCode){case 32:case 13:a=this._buttonToggles.get(o)||null;break;case 38:a=this._getNextButton(o,-1);break;case 37:a=this._getNextButton(o,this.dir==="ltr"?-1:1);break;case 40:a=this._getNextButton(o,1);break;case 39:a=this._getNextButton(o,this.dir==="ltr"?1:-1);break;default:return}a&&(e.preventDefault(),a._onButtonClick(),a.focus())}_emitChangeEvent(e){let r=new bu(e,this.value);this._rawValue=r.value,this._controlValueAccessorChangeFn(r.value),this.change.emit(r)}_syncButtonToggle(e,r,i=!1,o=!1){!this.multiple&&this.selected&&!e.checked&&(this.selected.checked=!1),this._selectionModel?r?this._selectionModel.select(e):this._selectionModel.deselect(e):o=!0,o?Promise.resolve().then(()=>this._updateModelValue(e,i)):this._updateModelValue(e,i)}_isSelected(e){return this._selectionModel&&this._selectionModel.isSelected(e)}_isPrechecked(e){return typeof this._rawValue>"u"?!1:this.multiple&&Array.isArray(this._rawValue)?this._rawValue.some(r=>e.value!=null&&r===e.value):e.value===this._rawValue}_initializeTabIndex(){if(this._buttonToggles.forEach(e=>{e.tabIndex=-1}),this.selected)this.selected.tabIndex=0;else for(let e=0;e<this._buttonToggles.length;e++){let r=this._buttonToggles.get(e);if(!r.disabled){r.tabIndex=0;break}}}_getNextButton(e,r){let i=this._buttonToggles;for(let o=1;o<=i.length;o++){let a=(e+r*o+i.length)%i.length,s=i.get(a);if(s&&!s.disabled)return s}return null}_setSelectionByValue(e){if(this._rawValue=e,!this._buttonToggles)return;let r=this._buttonToggles.toArray();if(this.multiple&&e?(Array.isArray(e),this._clearSelection(),e.forEach(i=>this._selectValue(i,r))):(this._clearSelection(),this._selectValue(e,r)),!this.multiple&&r.every(i=>i.tabIndex===-1)){for(let i of r)if(!i.disabled){i.tabIndex=0;break}}}_clearSelection(){this._selectionModel.clear(),this._buttonToggles.forEach(e=>{e.checked=!1,this.multiple||(e.tabIndex=-1)})}_selectValue(e,r){for(let i of r)if(i.value===e){i.checked=!0,this._selectionModel.select(i),this.multiple||(i.tabIndex=0);break}}_updateModelValue(e,r){r&&this._emitChangeEvent(e),this.valueChange.emit(this.value)}_markButtonsForCheck(){this._buttonToggles?.forEach(e=>e._markForCheck())}static \u0275fac=function(r){return new(r||t)};static \u0275dir=j({type:t,selectors:[["mat-button-toggle-group"]],contentQueries:function(r,i,o){if(r&1&&Ln(o,vu,5),r&2){let a;ae(a=se())&&(i._buttonToggles=a)}},hostAttrs:[1,"mat-button-toggle-group"],hostVars:6,hostBindings:function(r,i){r&1&&B("keydown",function(a){return i._keydown(a)}),r&2&&(de("role",i.multiple?"group":"radiogroup")("aria-disabled",i.disabled),U("mat-button-toggle-vertical",i.vertical)("mat-button-toggle-group-appearance-standard",i.appearance==="standard"))},inputs:{appearance:"appearance",name:"name",vertical:[2,"vertical","vertical",N],value:"value",multiple:[2,"multiple","multiple",N],disabled:[2,"disabled","disabled",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",N],hideMultipleSelectionIndicator:[2,"hideMultipleSelectionIndicator","hideMultipleSelectionIndicator",N]},outputs:{valueChange:"valueChange",change:"change"},exportAs:["matButtonToggleGroup"],features:[ke([ZR,{provide:SD,useExisting:t}])]})}return t})(),vu=(()=>{class t{_changeDetectorRef=u(ve);_elementRef=u(V);_focusMonitor=u(en);_idGenerator=u(je);_animationDisabled=Be();_checked=!1;ariaLabel;ariaLabelledby=null;_buttonElement;buttonToggleGroup;get buttonId(){return`${this.id}-button`}id;name;value;get tabIndex(){return this._tabIndex()}set tabIndex(e){this._tabIndex.set(e)}_tabIndex;disableRipple=!1;get appearance(){return this.buttonToggleGroup?this.buttonToggleGroup.appearance:this._appearance}set appearance(e){this._appearance=e}_appearance;get checked(){return this.buttonToggleGroup?this.buttonToggleGroup._isSelected(this):this._checked}set checked(e){e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&this.buttonToggleGroup._syncButtonToggle(this,this._checked),this._changeDetectorRef.markForCheck())}get disabled(){return this._disabled||this.buttonToggleGroup&&this.buttonToggleGroup.disabled}set disabled(e){this._disabled=e}_disabled=!1;get disabledInteractive(){return this._disabledInteractive||this.buttonToggleGroup!==null&&this.buttonToggleGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new C;constructor(){u(Me).load(nn);let e=u(SD,{optional:!0}),r=u(new jt("tabindex"),{optional:!0})||"",i=u(DD,{optional:!0});this._tabIndex=A(parseInt(r)||0),this.buttonToggleGroup=e,this._appearance=i&&i.appearance?i.appearance:"standard",this._disabledInteractive=i?.disabledInteractive??!1}ngOnInit(){let e=this.buttonToggleGroup;this.id=this.id||this._idGenerator.getId("mat-button-toggle-"),e&&(e._isPrechecked(this)?this.checked=!0:e._isSelected(this)!==this._checked&&e._syncButtonToggle(this,this._checked))}ngAfterViewInit(){this._animationDisabled||this._elementRef.nativeElement.classList.add("mat-button-toggle-animations-enabled"),this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){let e=this.buttonToggleGroup;this._focusMonitor.stopMonitoring(this._elementRef),e&&e._isSelected(this)&&e._syncButtonToggle(this,!1,!1,!0)}focus(e){this._buttonElement.nativeElement.focus(e)}_onButtonClick(){if(this.disabled)return;let e=this.isSingleSelector()?!0:!this._checked;if(e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&(this.buttonToggleGroup._syncButtonToggle(this,this._checked,!0),this.buttonToggleGroup._onTouched())),this.isSingleSelector()){let r=this.buttonToggleGroup._buttonToggles.find(i=>i.tabIndex===0);r&&(r.tabIndex=-1),this.tabIndex=0}this.change.emit(new bu(this,this.value))}_markForCheck(){this._changeDetectorRef.markForCheck()}_getButtonName(){return this.isSingleSelector()?this.buttonToggleGroup.name:this.name||null}isSingleSelector(){return this.buttonToggleGroup&&!this.buttonToggleGroup.multiple}static \u0275fac=function(r){return new(r||t)};static \u0275cmp=(function(){let e=["button"],r=["*"];function i(o,a){if(o&1&&(v(0,"div",2),Z(1,"mat-pseudo-checkbox",6),y()),o&2){let s=fe();_(),z("disabled",s.disabled)}}return k({type:t,selectors:[["mat-button-toggle"]],viewQuery:function(a,s){if(a&1&&He(e,5),a&2){let l;ae(l=se())&&(s._buttonElement=l.first)}},hostAttrs:["role","presentation",1,"mat-button-toggle"],hostVars:14,hostBindings:function(a,s){a&1&&B("focus",function(){return s.focus()}),a&2&&(de("aria-label",null)("aria-labelledby",null)("id",s.id)("name",null),U("mat-button-toggle-standalone",!s.buttonToggleGroup)("mat-button-toggle-checked",s.checked)("mat-button-toggle-disabled",s.disabled)("mat-button-toggle-disabled-interactive",s.disabledInteractive)("mat-button-toggle-appearance-standard",s.appearance==="standard"))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],id:"id",name:"name",value:"value",tabIndex:"tabIndex",disableRipple:[2,"disableRipple","disableRipple",N],appearance:"appearance",checked:[2,"checked","checked",N],disabled:[2,"disabled","disabled",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},outputs:{change:"change"},exportAs:["matButtonToggle"],ngContentSelectors:r,decls:7,vars:13,consts:[["button",""],["type","button",1,"mat-button-toggle-button","mat-focus-indicator",3,"click","id","disabled"],[1,"mat-button-toggle-checkbox-wrapper"],[1,"mat-button-toggle-label-content"],[1,"mat-button-toggle-focus-overlay"],["matRipple","",1,"mat-button-toggle-ripple",3,"matRippleTrigger","matRippleDisabled"],["state","checked","aria-hidden","true","appearance","minimal",3,"disabled"]],template:function(a,s){if(a&1&&(Oe(),v(0,"button",1,0),B("click",function(){return s._onButtonClick()}),xe(2,i,2,1,"div",2),v(3,"span",3),ce(4),y()(),Z(5,"span",4)(6,"span",5)),a&2){let l=Zt(1);z("id",s.buttonId)("disabled",s.disabled&&!s.disabledInteractive||null),de("role",s.isSingleSelector()?"radio":"button")("tabindex",s.disabled&&!s.disabledInteractive?-1:s.tabIndex)("aria-pressed",s.isSingleSelector()?null:s.checked)("aria-checked",s.isSingleSelector()?s.checked:null)("name",s._getButtonName())("aria-label",s.ariaLabel)("aria-labelledby",s.ariaLabelledby)("aria-disabled",s.disabled&&s.disabledInteractive?"true":null),_(2),we(s.buttonToggleGroup&&(!s.buttonToggleGroup.multiple&&!s.buttonToggleGroup.hideSingleSelectionIndicator||s.buttonToggleGroup.multiple&&!s.buttonToggleGroup.hideMultipleSelectionIndicator)?2:-1),_(4),z("matRippleTrigger",l)("matRippleDisabled",s.disableRipple||s.disabled)}},dependencies:[Mr,xD],styles:[`.mat-button-toggle-standalone,
.mat-button-toggle-group {
  position: relative;
  display: inline-flex;
  flex-direction: row;
  white-space: nowrap;
  overflow: hidden;
  -webkit-tap-highlight-color: transparent;
  border-radius: var(--%NS%mat-button-toggle-legacy-shape);
  transform: translateZ(0);
}
.mat-button-toggle-standalone:not([class*=mat-elevation-z]),
.mat-button-toggle-group:not([class*=mat-elevation-z]) {
  box-shadow: 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12);
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone,
  .mat-button-toggle-group {
    outline: solid 1px;
  }
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
.mat-button-toggle-group-appearance-standard {
  border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard .mat-pseudo-checkbox,
.mat-button-toggle-group-appearance-standard .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard:not([class*=mat-elevation-z]),
.mat-button-toggle-group-appearance-standard:not([class*=mat-elevation-z]) {
  box-shadow: none;
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
  .mat-button-toggle-group-appearance-standard {
    outline: 0;
  }
}

.mat-button-toggle-vertical {
  flex-direction: column;
}
.mat-button-toggle-vertical .mat-button-toggle-label-content {
  display: block;
}

.mat-button-toggle {
  white-space: nowrap;
  position: relative;
  color: var(--%NS%mat-button-toggle-legacy-text-color);
  font-family: var(--%NS%mat-button-toggle-legacy-label-text-font);
  font-size: var(--%NS%mat-button-toggle-legacy-label-text-size);
  line-height: var(--%NS%mat-button-toggle-legacy-label-text-line-height);
  font-weight: var(--%NS%mat-button-toggle-legacy-label-text-weight);
  letter-spacing: var(--%NS%mat-button-toggle-legacy-label-text-tracking);
  --%NS%mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
}
.mat-button-toggle.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-legacy-focus-state-layer-opacity);
}
.mat-button-toggle .mat-icon svg {
  vertical-align: top;
}

.mat-button-toggle-checkbox-wrapper {
  display: inline-block;
  justify-content: flex-start;
  align-items: center;
  width: 0;
  height: 18px;
  line-height: 18px;
  overflow: hidden;
  box-sizing: border-box;
  position: absolute;
  top: 50%;
  left: 16px;
  transform: translate3d(0, -50%, 0);
}
[dir=rtl] .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 16px;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: 12px;
}
[dir=rtl] .mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 12px;
}
.mat-button-toggle-checked .mat-button-toggle-checkbox-wrapper {
  width: 18px;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-checkbox-wrapper {
  transition: width 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-checkbox-wrapper {
  transition: none;
}

.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-legacy-selected-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-selected-state-background-color);
}

.mat-button-toggle-disabled {
  pointer-events: none;
  color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-state-background-color);
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-legacy-disabled-state-text-color);
}
.mat-button-toggle-disabled.mat-button-toggle-checked {
  background-color: var(--%NS%mat-button-toggle-legacy-disabled-selected-state-background-color);
}

.mat-button-toggle-disabled-interactive {
  pointer-events: auto;
}

.mat-button-toggle-appearance-standard {
  color: var(--%NS%mat-button-toggle-text-color, var(--%NS%mat-sys-on-surface));
  background-color: var(--%NS%mat-button-toggle-background-color, transparent);
  font-family: var(--%NS%mat-button-toggle-label-text-font, var(--%NS%mat-sys-label-large-font));
  font-size: var(--%NS%mat-button-toggle-label-text-size, var(--%NS%mat-sys-label-large-size));
  line-height: var(--%NS%mat-button-toggle-label-text-line-height, var(--%NS%mat-sys-label-large-line-height));
  font-weight: var(--%NS%mat-button-toggle-label-text-weight, var(--%NS%mat-sys-label-large-weight));
  letter-spacing: var(--%NS%mat-button-toggle-label-text-tracking, var(--%NS%mat-sys-label-large-tracking));
}
.mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
[dir=rtl] .mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: none;
  border-top: solid 1px var(--%NS%mat-button-toggle-divider-color, var(--%NS%mat-sys-outline));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-selected-state-text-color, var(--%NS%mat-sys-on-secondary-container));
  background-color: var(--%NS%mat-button-toggle-selected-state-background-color, var(--%NS%mat-sys-secondary-container));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled {
  color: var(--%NS%mat-button-toggle-disabled-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-state-background-color, transparent);
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled .mat-pseudo-checkbox {
  --%NS%mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled.mat-button-toggle-checked {
  color: var(--%NS%mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 38%, transparent));
  background-color: var(--%NS%mat-button-toggle-disabled-selected-state-background-color, color-mix(in srgb, var(--%NS%mat-sys-on-surface) 12%, transparent));
}
.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
  background-color: var(--%NS%mat-button-toggle-state-layer-color, var(--%NS%mat-sys-on-surface));
}
.mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-hover-state-layer-opacity, var(--%NS%mat-sys-hover-state-layer-opacity));
}
.mat-button-toggle-appearance-standard.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--%NS%mat-button-toggle-focus-state-layer-opacity, var(--%NS%mat-sys-focus-state-layer-opacity));
}
@media (hover: none) {
  .mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
    display: none;
  }
}

.mat-button-toggle-label-content {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  padding: 0 16px;
  line-height: var(--%NS%mat-button-toggle-legacy-height);
  position: relative;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-label-content {
  padding: 0 12px;
  line-height: var(--%NS%mat-button-toggle-height, 40px);
}

.mat-button-toggle-label-content > * {
  vertical-align: middle;
}

.mat-button-toggle-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  background-color: var(--%NS%mat-button-toggle-legacy-state-layer-color);
}

@media (forced-colors: active) {
  .mat-button-toggle-checked .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
    opacity: 0.5;
    height: 0;
  }
  .mat-button-toggle-checked:hover .mat-button-toggle-focus-overlay {
    opacity: 0.6;
  }
  .mat-button-toggle-checked.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
  }
}
.mat-button-toggle .mat-button-toggle-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-button-toggle-button {
  border: 0;
  background: none;
  color: inherit;
  padding: 0;
  margin: 0;
  font: inherit;
  outline: none;
  width: 100%;
  cursor: pointer;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-button {
  transition: padding 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-button {
  transition: none;
}
.mat-button-toggle-disabled .mat-button-toggle-button {
  cursor: default;
}
.mat-button-toggle-button::-moz-focus-inner {
  border: 0;
}
.mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 30px;
}
[dir=rtl] .mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 0;
  padding-right: 30px;
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard {
  --%NS%mat-focus-indicator-border-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-bottom-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
  border-top-left-radius: var(--%NS%mat-button-toggle-shape, var(--%NS%mat-sys-corner-extra-large));
}
`],encapsulation:2})})()}return t})(),ED=(()=>{class t{static \u0275fac=function(r){return new(r||t)};static \u0275mod=oe({type:t});static \u0275inj=re({imports:[Fo,vu,Fe]})}return t})();var yu=class t{static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-egarita-addition"]],decls:15,vars:0,consts:[["appearance","outlined"],["name","favoriteColor","aria-label","Favorite Color"],["value","One"],["value","Two"],["value","Three"]],template:function(e,r){e&1&&(v(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),M(3," How many pizzas would you like "),y()(),v(4,"mat-card-content")(5,"section")(6,"h3"),M(7,"How many pizzas would you like"),y(),v(8,"mat-button-toggle-group",1)(9,"mat-button-toggle",2),M(10,"One"),y(),v(11,"mat-button-toggle",3),M(12,"Two"),y(),v(13,"mat-button-toggle",4),M(14,"Three"),y()()()()())},dependencies:[Bn,Ud,an,At,Rt,on,rn,ED,Dg,vu],encapsulation:2})};var _u=class t{numberOne=A(1);numberTwo=A(1);sum=A(2);updateNumberOne=n=>this.numberOne.set(Number(n));updateNumberTwo=n=>this.numberTwo.set(Number(n));add=()=>this.sum.set(this.numberOne()+this.numberTwo());static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-tsteele-addition"]],decls:19,vars:3,consts:[["appearance","outlined"],["type","number","matInput","",3,"input","value"],["matButton","outlined",3,"click"]],template:function(e,r){e&1&&(v(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),M(3," Tom's Addition Component "),y()(),v(4,"mat-card-content"),Z(5,"br"),v(6,"mat-form-field")(7,"mat-label"),M(8," Number One "),y(),v(9,"input",1),B("input",function(o){return r.updateNumberOne(o.target.value)}),y()(),M(10," + "),v(11,"mat-form-field")(12,"mat-label"),M(13," Number Two "),y(),v(14,"input",1),B("input",function(o){return r.updateNumberTwo(o.target.value)}),y()(),M(15," \xA0 "),v(16,"button",2),B("click",function(){return r.add()}),M(17," = "),y(),M(18),y()()),e&2&&(_(9),z("value",r.numberOne()),_(5),z("value",r.numberTwo()),_(4),St(" \xA0 ",r.sum()," "))},dependencies:[an,At,Rt,on,rn,Bn,nr,or,Tr,ir,Ho,zo],encapsulation:2})};var xu=class t{orderTotal=A(.99);taxSum=A(1.04);updateOrderTotal=n=>this.orderTotal.set(Number(n));add=()=>this.taxSum.set(this.orderTotal()*1.05);static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-jebashel-tax"]],decls:14,vars:4,consts:[["appearance","outlined"],["type","number","matInput","",3,"input","value"],["matButton","outlined",3,"click"]],template:function(e,r){e&1&&(v(0,"mat-card",0)(1,"mat-card-header")(2,"mat-card-title"),M(3," Calculate Tax... "),y()(),v(4,"mat-card-content")(5,"mat-form-field")(6,"mat-label"),M(7," Enter your total to add the tax "),y(),v(8,"input",1),B("input",function(o){return r.updateOrderTotal(o.target.value)}),y()(),M(9," \xA0 "),v(10,"button",2),B("click",function(){return r.add()}),M(11," Add tax (5%) "),y(),M(12),mo(13,"currency"),y()()),e&2&&(_(8),z("value",r.orderTotal()),_(4),St(" \xA0 ",fo(13,2,r.taxSum())," "))},dependencies:[At,Rt,rn,on,Bn,nr,or,Tr,ir,Ho,zo,rs],styles:["mat-card-header[_ngcontent-%COMP%]{margin-bottom:20px}"]})};var wu=class t{title=A("angular-components");static \u0275fac=function(e){return new(e||t)};static \u0275cmp=k({type:t,selectors:[["app-root"]],decls:9,vars:0,template:function(e,r){e&1&&(v(0,"h1"),M(1,` Our components...
`),y(),Z(2,"app-tsteele-addition")(3,"app-pizza-toppings")(4,"app-egarita-addition")(5,"app-crust-selection")(6,"app-elee-date-picker")(7,"app-leave-tip")(8,"app-jebashel-tax"))},dependencies:[$d,qd,pu,gu,yu,_u,xu],encapsulation:2})};Rp(wu,gw).catch(t=>console.error(t));
