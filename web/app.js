'use strict';
let token=sessionStorage.getItem('afterzero-token')||'',scopes=[],messages=[],busy=false,selected=null;
const $=id=>document.getElementById(id);
const esc=x=>String(x).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels={waiting_for_coverage:'Waiting for coverage',unchanged_through_checkpoint:'Covered through checkpoint',revoked:'Revoked',unknown:'Unknown · revision gap',recheck_required:'Recheck required',expired:'Expired'};
function notice(text,error=false){$('notice').textContent=text;$('notice').className=error?'error':'';}
async function api(path,body,method){const r=await fetch('/api'+path,{method:method||(body?'POST':'GET'),headers:{'Content-Type':'application/json',...(token?{Authorization:'Bearer '+token}:{})},...(body?{body:JSON.stringify(body)}:{})});const d=await r.json();if(!r.ok)throw Error(d.error||'Request failed');return d;}
function lock(on){busy=on;document.querySelectorAll('[data-action],#refresh,#restart').forEach(b=>b.disabled=on||!token);$('start').disabled=on;}
function explain(r){const x=r.reason;if(x.code==='matching_late_record')return 'A late row ('+(x.witness.id||'unnamed')+') contradicted this check at '+x.producer+' revision '+x.revision+'.';if(x.code==='incomplete_producers')return 'Awaiting completion from '+x.producers.join(', ')+'. Zero failures is not enough.';if(x.code==='revision_gap')return 'Received later revisions before their predecessors. Coverage cannot be asserted.';if(x.code==='covered')return 'Required inputs are complete. No contradicting change through the listed revisions.';if(x.code==='coarse_source_change')return 'Source changed without exact row evidence. Recheck in the source system.';if(x.code==='coverage_withdrawn')return 'A producer withdrew a completeness assertion.';return 'The receipt monitoring period has ended.';}
function render(){
$('session-label').textContent=token?'Isolated live session · 1 hour':'No active session';
$('cards').innerHTML=scopes.map(s=>{const r=s.receipts[0];if(!r)return '';return '<article class="receipt"><div class="receipt-top"><span>DAILY IMPORT / '+esc(s.partition.business_date)+'</span><span class="customer">'+esc(s.partition.customer)+'</span></div><h4>Customer '+esc(s.partition.customer)+'</h4><span class="badge '+esc(r.status)+'">'+esc(labels[r.status])+'</span><div class="reason">'+esc(explain(r))+'</div><div class="facts"><div><span>Observed failures</span><strong>0 at registration</strong></div><div><span>Source revisions</span><strong>U:'+s.heads.uploader+' / V:'+s.heads.validator+'</strong></div><div><span>Inputs complete</span><strong>'+Object.values(s.complete).filter(Boolean).length+' / 2 producers</strong></div><div><span>Receipt</span><strong>'+esc(r.receipt_id)+'</strong></div></div><div class="card-actions"><button data-inspect="'+esc(s.id)+'">Inspect receipt</button><button data-download="'+esc(s.id)+'">Export report</button></div></article>';}).join('');
const events=scopes.flatMap(s=>s.commits.map(e=>({...e,customer:s.partition.customer}))).sort((a,b)=>b.applied_at-a.applied_at||b.revision-a.revision);
$('event-total').textContent=events.length+' COMMITS';
$('timeline').innerHTML=events.map(e=>'<div class="timeline-item"><span class="seq">r'+e.revision+'</span><div>Customer '+esc(e.customer)+' · '+esc(e.producer)+'<small>'+esc(e.event_id)+' · '+e.rows.length+' projected rows</small></div><span class="type">'+esc(e.kind.toUpperCase())+'</span></div>').join('');
$('inbox').innerHTML=messages.length?messages.slice(0,12).map(m=>'<div class="inbox-item"><div>'+esc(m.scope_id)+' · snapshot '+m.version+'<small>SHA256 '+esc(m.sha256.slice(0,18))+'…</small></div><div><button data-evidence="'+esc(m.evidence_key)+'">Evidence</button> <button data-ack="'+esc(m.id)+'" '+(m.acknowledged?'disabled':'')+'>'+(m.acknowledged?'Received ✓':'Acknowledge')+'</button></div></div>').join(''):'<p class="muted">Waiting for the evidence workflow. Refresh to retrieve its delivery.</p>';
if(selected)inspect(selected);
}
function inspect(id){selected=id;const s=scopes.find(x=>x.id===id);if(!s)return;$('inspect-label').textContent=id.toUpperCase();$('inspector').textContent=JSON.stringify(s.receipts[0],null,2);}
async function refresh(){scopes=(await api('/scopes')).scopes;messages=(await api('/inbox')).messages;render();}
async function start(){lock(true);notice('Creating isolated scopes and evaluating initial data on AWS…');try{token='';const d=await api('/demo',{});token=d.token;sessionStorage.setItem('afterzero-token',token);scopes=d.scopes;selected='customer-A';await refresh();$('start').textContent='Workspace connected ✓';notice('Two checks returned zero failures, but the validator has not completed its input. Both receipts correctly wait for coverage.');$('workspace').scrollIntoView({behavior:'smooth'});}catch(e){notice(e.message,true);}finally{lock(false);}}
async function waitFor(test){for(let i=0;i<12;i++){await refresh();if(test())return;await new Promise(r=>setTimeout(r,750));}throw Error('AWS is still processing the manifest. Use Refresh to check its status.');}
const scopeA=()=>scopes.find(s=>s.id==='customer-A'),scopeB=()=>scopes.find(s=>s.id==='customer-B');
async function act(action){if(busy)return;lock(true);try{
if(action==='cover'){for(const s of scopes)await api('/scopes/'+s.id+'/events',{event_id:'validator-seal',producer:'validator',revision:1,kind:'seal'});notice('Both producers declared completion. The receipt cards show whether later evidence affects their coverage.');}
if(action==='late'){if(scopeA().heads.validator<1)throw Error('Complete the inputs first to see a covered receipt become revoked.');await api('/scopes/customer-A/manifests',{event_id:'late-invalid-A',producer:'uploader',revision:2,kind:'append',rows:[{id:'late-row-A',validation_status:'invalid'}],complete:true});notice('Manifest stored in S3. Waiting for S3 → Lambda ingestion…');await waitFor(()=>scopeA().heads.uploader>=2);notice('The late record revoked customer A’s receipt. This event did not change customer B’s state. The old report can now be identified as superseded.');}
if(action==='duplicate'){if(scopeA().heads.uploader<2)throw Error('Deliver the late invalid row first.');const d=await api('/scopes/customer-A/events',{event_id:'late-invalid-A',producer:'uploader',revision:2,kind:'append',rows:[{id:'late-row-A',validation_status:'invalid'}],complete:true});notice(d.duplicate?'Duplicate accepted idempotently: no extra revision, state mutation, or evidence workflow.':'Event applied.');}
if(action==='gap'){if(scopeB().heads.validator<1)throw Error('Complete the inputs first.');await api('/scopes/customer-B/events',{event_id:'out-of-order-B',producer:'uploader',revision:3,kind:'seal'});notice('Customer B received revision 3 before revision 2. It is Unknown, even though the check previously passed.');}
if(action==='repair'){if(!scopeB().pending_count)throw Error('Skip a source revision first.');await api('/scopes/customer-B/events',{event_id:'missing-revision-B',producer:'uploader',revision:2,kind:'append',rows:[{id:'recovered-B',validation_status:'valid'}],complete:true});notice('Revision 2 closed the gap. The service applied buffered revision 3 and restored customer B’s coverage.');}
await refresh();
}catch(e){notice(e.message,true);}finally{lock(false);}}
function download(id){const s=scopes.find(x=>x.id===id),r=s.receipts[0];const report={title:'Daily validation report — customer '+s.partition.customer,exported_at:new Date().toISOString(),historical_observation:'Zero failing records at the source basis below',receipt:r,scope_id:id,status_endpoint:location.origin+'/api/scopes/'+id,usage:'This is a point-in-time export. Authenticate and check status before reusing. Tokens are not included.'};const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([JSON.stringify(report,null,2)],{type:'application/json'}));a.download='afterzero-report-'+s.partition.customer+'.json';a.click();URL.revokeObjectURL(a.href);notice('Report exported with its receipt, source basis, and status endpoint. No access token is included.');}
$('start').addEventListener('click',start);$('restart').addEventListener('click',start);
$('refresh').addEventListener('click',async()=>{lock(true);try{await refresh();notice('Latest receipt state and workflow deliveries retrieved from AWS.');}catch(e){notice(e.message,true);}finally{lock(false);}});
document.addEventListener('click',async e=>{const b=e.target.closest('button');if(!b)return;if(b.dataset.action)act(b.dataset.action);if(b.dataset.inspect){inspect(b.dataset.inspect);$('inspector').scrollIntoView({behavior:'smooth',block:'center'});}if(b.dataset.download)download(b.dataset.download);if(b.dataset.ack){try{await api('/ack',{id:b.dataset.ack});await refresh();notice('Consumer acknowledgement persisted. This confirms receipt, not downstream repair.');}catch(e){notice(e.message,true);}}if(b.dataset.evidence){try{const d=await api('/evidence?key='+encodeURIComponent(b.dataset.evidence));selected=null;$('inspect-label').textContent='S3 EVIDENCE SNAPSHOT';$('inspector').textContent=JSON.stringify(d,null,2);$('inspector').scrollIntoView({behavior:'smooth',block:'center'});}catch(e){notice(e.message,true);}}});
if(token){lock(true);refresh().then(()=>{$('start').textContent='Start fresh workspace';notice('Restored your live AWS session. Receipt expiry is evaluated on every read.');}).catch(()=>{token='';sessionStorage.removeItem('afterzero-token');notice('Session expired. Open a fresh workspace.');}).finally(()=>lock(false));}


// The introduction carousel is explanatory; live receipt state is always returned by AWS.
const introductions=[
  {title:'AfterZero',description:'Know when a passed data check\nstops telling the whole story'},
  {title:'Beyond zero',description:'A result depends on its inputs\nMake those dependencies visible'},
  {title:'Follow the evidence',description:'Every change · Every checkpoint\nAn answer you can inspect'}
];
let introduction=0;
function showIntroduction(index){
  introduction=(index+introductions.length)%introductions.length;
  document.querySelector('.hero').dataset.slide=String(introduction);
  $('hero-title').textContent=introductions[introduction].title;
  $('hero-description').textContent=introductions[introduction].description;
  document.querySelectorAll('.hero-dots button').forEach((b,i)=>{b.classList.toggle('is-active',i===introduction);b.setAttribute('aria-pressed',String(i===introduction));});
}
$('hero-prev').addEventListener('click',()=>showIntroduction(introduction-1));
$('hero-next').addEventListener('click',()=>showIntroduction(introduction+1));
document.querySelectorAll('.hero-dots button').forEach(b=>b.addEventListener('click',()=>showIntroduction(Number(b.dataset.slide))));
