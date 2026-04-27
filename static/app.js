const $ = id => document.getElementById(id);
let buckets = [];
let conversations = [];
let hiddenSeries = new Set();
let liveMode = true;
let viewportOffset = 0;
let egressFilter = "";
let chartGeom = null;
let brushDrag = null;
let pausedCharts = false;
let frozenBuckets = [];
const BASE_VIEWPORT_SECONDS = 120;
const colors = ['#6ee7b7','#60a5fa','#c084fc','#fbbf24','#fb7185','#34d399','#38bdf8','#f472b6','#a3e635','#f97316','#818cf8','#22d3ee','#f59e0b','#2dd4bf','#e879f9','#93c5fd','#fdba74','#86efac','#67e8f9','#f0abfc'];

function cfg(){return{host:$('host').value.trim(),vdom:$('vdom').value.trim()||'root',apiToken:$('token').value,username:$('username').value,password:$('password').value,insecureTLS:$('insecure').checked,pollSeconds:+$('poll').value,windowMinutes:+$('window').value,groupBy:'conversation',topN:+$('topn').value,demoMode:$('demo').checked,endpoint:$('endpoint').value.trim(),resolveDevices:$('resolveDevices').checked,deviceResolveSeconds:+$('deviceResolveSeconds').value,deviceEndpoint:$('deviceEndpoint').value.trim(),deviceCommand:$('deviceCommand').value.trim(),resolveExternalDns:$('resolveExternalDns').checked,dnsServer:$('dnsServer').value.trim(),dnsCacheMinutes:+$('dnsCacheMinutes').value,saveSettings:$('saveSettings').checked,saveSecrets:$('saveSecrets').checked}}
async function post(url,data){const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:data?JSON.stringify(data):'{}'});if(!r.ok)throw new Error(await r.text());return r.json()}
function setVal(id,val){const el=$(id);if(!el||val===undefined||val===null)return;if(el.type==='checkbox')el.checked=!!val;else el.value=val}
function hydrateConfig(c){if(!c)return;if(c.deviceEndpoint && (c.deviceEndpoint.includes('/api/v2/monitor/system') || c.deviceEndpoint.includes('/console'))) c.deviceEndpoint='/api/v2/monitor/user/device/query';setVal('host',c.host);setVal('vdom',c.vdom||'root');setVal('username',c.username);setVal('insecure',c.insecureTLS);setVal('poll',c.pollSeconds);setVal('window',c.windowMinutes);setVal('topn',c.topN);setVal('demo',c.demoMode);setVal('endpoint',c.endpoint);setVal('resolveDevices',c.resolveDevices);setVal('deviceResolveSeconds',c.deviceResolveSeconds);setVal('deviceEndpoint',c.deviceEndpoint);setVal('deviceCommand',c.deviceCommand);setVal('resolveExternalDns',c.resolveExternalDns);setVal('dnsServer',c.dnsServer);setVal('dnsCacheMinutes',c.dnsCacheMinutes);setVal('saveSettings',c.saveSettings);setVal('saveSecrets',c.saveSecrets)}

$('saveStart').onclick=async()=>{try{hiddenSeries.clear();liveMode=true;viewportOffset=0;await post('/api/config',cfg());await post('/api/start');setStatus('Started. First poll establishes a baseline; bandwidth appears after the second poll.')}catch(e){setStatus('ERROR: '+e.message)}};
$('stop').onclick=async()=>{await post('/api/stop')};
$('clear').onclick=()=>{buckets=[];frozenBuckets=[];drawAll()};
$('showAll').onclick=()=>{filteredConversations().forEach(c=>hiddenSeries.delete(c.label));drawAll();renderConversations()};
$('hideAll').onclick=()=>{filteredConversations().forEach(c=>hiddenSeries.add(c.label));drawAll();renderConversations()};
$('goLive').onclick=()=>{pausedCharts=false;frozenBuckets=[];updatePauseButton();liveMode=true;viewportOffset=0;drawAll()};
$('egressFilter').onchange=()=>{egressFilter=$('egressFilter').value;drawAll();renderConversations();updateFilterStatus()};
$('clearEgressFilter').onclick=()=>{egressFilter='';$('egressFilter').value='';drawAll();renderConversations();updateFilterStatus()};

$('pauseCharts').onclick=()=>{
  pausedCharts=!pausedCharts;
  if(pausedCharts){
    frozenBuckets=JSON.parse(JSON.stringify(buckets||[]));
    liveMode=false;
  } else {
    frozenBuckets=[];
    liveMode=true;
    viewportOffset=0;
  }
  updatePauseButton();
  drawAll();
};


const chartEl = $('chart');
const miniEl = $('miniChart');
chartEl.addEventListener('mousemove', onChartMove);
chartEl.addEventListener('mouseleave', hideTooltip);
miniEl.addEventListener('mousedown', onMiniDown);
window.addEventListener('mousemove', onMiniMove);
window.addEventListener('mouseup', onMiniUp);

function setStatus(s){$('status').textContent=s}
function connect(){const es=new EventSource('/events');es.onmessage=e=>apply(JSON.parse(e.data));es.onerror=()=>setStatus('Event stream reconnecting…')}
async function load(){const r=await fetch('/api/snapshot');apply(await r.json())}
let hydrated=false;
function apply(s){
  if(!hydrated){hydrateConfig(s.config);hydrated=true;}
  buckets=s.buckets||[];conversations=s.conversations||[];const st=s.status||{};
  $('statePill').textContent=st.running?'Running':'Stopped';$('statePill').className='pill '+(st.running?'running':'');
  $('total').textContent=fmtMbps(st.totalMbps||0);$('sessions').textContent=(st.sessionCount||0).toLocaleString();$('lastPoll').textContent=st.lastPoll?new Date(st.lastPoll).toLocaleTimeString():'Never';
  $('subtitle').textContent=buckets.length?`${buckets.length} retained buckets. Displaying a ${Math.round(viewportSeconds()/60*10)/10}-minute ${pausedCharts?'paused':(liveMode?'live':'historical')} viewport.`:'Waiting for two successful polls…';
  setStatus(st.lastError?`Last error: ${st.lastError}`:(st.deviceResolverStatus?`Healthy. ${st.deviceResolverStatus}`:'Healthy.'));
  refreshEgressFilterOptions();clampViewport();drawAll();renderConversations();
}



function chartBuckets(){return pausedCharts ? frozenBuckets : buckets}
function viewportSeconds(){const w=chartEl?.clientWidth||1000;return Math.max(60, Math.round((w/1000)*BASE_VIEWPORT_SECONDS));}
function updatePauseButton(){const b=$('pauseCharts'); if(!b)return; b.textContent=pausedCharts?'Resume live charts':'Pause'; b.className='ghost '+(pausedCharts?'active':'');}

function conversationMap(){const m=new Map(); conversations.forEach(c=>m.set(c.label,c)); return m}
function availableEgressInterfaces(){return [...new Set(conversations.map(c=>(c.egressInterface||'').trim()).filter(Boolean))].sort((a,b)=>a.localeCompare(b))}
function refreshEgressFilterOptions(){const sel=$('egressFilter'); if(!sel)return; const current=egressFilter || sel.value || ''; const interfaces=availableEgressInterfaces(); const opts=[...interfaces]; if(current && !opts.includes(current)) opts.unshift(current); sel.innerHTML='<option value="">All interfaces</option>'+opts.map(i=>`<option value="${escAttr(i)}">${esc(i)}${current===i&&!interfaces.includes(i)?' (no active rows)':''}</option>`).join(''); sel.value=current;}
function matchesEgress(label){if(!egressFilter)return true; const c=conversationMap().get(label); return !!c && (c.egressInterface||'')===egressFilter}
function matchesEgressInBucket(label,b){if(!egressFilter)return true; const fromBucket=(b.interfaces||{})[label]; if(fromBucket!==undefined && fromBucket!==null && fromBucket!=='') return fromBucket===egressFilter; const c=conversationMap().get(label); return !!c && (c.egressInterface||'')===egressFilter}
function filteredConversations(){return egressFilter ? conversations.filter(c=>(c.egressInterface||'')===egressFilter) : conversations.slice()}
function filteredBucketTotal(b){let total=0; const series=b.series||{}; Object.keys(series).forEach(k=>{if(k!=='Other' && matchesEgressInBucket(k,b) && !hiddenSeries.has(k)) total+=series[k]||0}); return total}
function updateFilterStatus(){const el=$('conversationCount'); if(!el)return;}

function fmtMbpsFromBps(bps){const mbps=(bps||0)/1e6;return mbps>=1000?(mbps/1000).toFixed(2)+' Gbps':mbps.toFixed(mbps>=10?1:2)+' Mbps'}
function fmtMbps(m){return m>=1000?(m/1000).toFixed(2)+' Gbps':m.toFixed(2)+' Mbps'}
function visibleKeys(){const bs=chartBuckets();const all=[...new Set(bs.flatMap(b=>Object.keys(b.series||{})))].sort();return all.filter(k=>!hiddenSeries.has(k) && k !== 'Other' && (!egressFilter || bs.some(b => (b.series||{})[k] !== undefined && matchesEgressInBucket(k,b))))}
function latestTs(){const bs=chartBuckets();return Math.max(pausedCharts?0:Math.floor(Date.now()/1000), ...(bs.map(b=>b.t||0)))}
function earliestTs(){const bs=chartBuckets();return bs.length?Math.min(...bs.map(b=>b.t||0)):latestTs()}
function maxOffset(){return Math.max(0, latestTs()-earliestTs()-viewportSeconds())}
function clampViewport(){const max=maxOffset();if(liveMode) viewportOffset=0; else viewportOffset=Math.min(Math.max(0,viewportOffset),max)}
function viewport(){clampViewport();const end=latestTs()-viewportOffset;const seconds=viewportSeconds();return {start:end-seconds,end,seconds}}
function drawAll(){draw();drawMini()}

function draw(){
  const svg=$('chart');svg.innerHTML='';const W=chartWidth(svg),H=430,pad={l:72,r:24,t:20,b:38};setSvgBox(svg,W,H);grid(svg,W,H,pad);
  const vp=viewport(); const keys=visibleKeys(); chartGeom=null;
  const bs=chartBuckets();
  if(bs.length<2 || keys.length===0){updateRangeLabel(vp);return;}
  const points = bs.filter(b => b.t >= vp.start-5 && b.t <= vp.end+5).sort((a,b)=>a.t-b.t);
  let max=0;
  const stacked=points.map(b=>{let y=0;const row={t:b.t,vals:{},bucket:b};keys.forEach(k=>{const v=matchesEgressInBucket(k,b)?((b.series||{})[k]||0):0;row.vals[k]=[y,y+v,v];y+=v});max=Math.max(max,y);return row});
  max=max||1;
  keys.forEach(k=>{let top=[],bot=[];stacked.forEach(r=>{const x=pad.l+((r.t-vp.start)/vp.seconds)*(W-pad.l-pad.r);const y1=pad.t+(1-r.vals[k][1]/max)*(H-pad.t-pad.b);const y0=pad.t+(1-r.vals[k][0]/max)*(H-pad.t-pad.b);top.push([x,y1]);bot.unshift([x,y0])});if(top.length){const d='M'+top.concat(bot).map(p=>p[0].toFixed(1)+','+p[1].toFixed(1)).join('L')+'Z';path(svg,d,colorForKey(k));}});
  chartGeom={W,H,pad,vp,keys,points:stacked,max};
  axisLabels(svg,W,H,pad,max,vp);updateRangeLabel(vp);
}
function drawMini(){
  const svg=$('miniChart');svg.innerHTML='';const W=chartWidth(svg),H=120,p={l:72,r:24,t:12,b:24};setSvgBox(svg,W,H);miniGrid(svg,W,H,p);
  const bs=chartBuckets();
  if(bs.length<2)return; const minT=earliestTs(), maxT=latestTs(); const span=Math.max(1,maxT-minT); let max=0;
  bs.forEach(b=>{max=Math.max(max,filteredBucketTotal(b))}); max=max||1;
  let d='';
  bs.slice().sort((a,b)=>a.t-b.t).forEach((b,i)=>{const x=p.l+((b.t-minT)/span)*(W-p.l-p.r);const y=p.t+(1-filteredBucketTotal(b)/max)*(H-p.t-p.b);d+=(i?'L':'M')+x.toFixed(1)+','+y.toFixed(1)});
  rect(svg,p.l,p.t,W-p.l-p.r,H-p.t-p.b,'brushTrack');
  strokePath(svg,d,'aggLine');
  const vp=viewport(); const x1=p.l+((vp.start-minT)/span)*(W-p.l-p.r); const x2=p.l+((vp.end-minT)/span)*(W-p.l-p.r);
  const bx=Math.max(p.l,Math.min(W-p.r,x1)); const bw=Math.max(8,Math.min(W-p.r,x2)-bx);
  rect(svg,bx,p.t,bw,H-p.t-p.b,'viewportMark');
  rect(svg,bx-4,p.t,8,H-p.t-p.b,'brushHandle');
  rect(svg,bx+bw-4,p.t,8,H-p.t-p.b,'brushHandle');
  miniAxis(svg,W,H,p,minT,maxT);
}
function updateRangeLabel(vp){$('rangeLabel').textContent= pausedCharts ? `Paused: ${new Date(vp.start*1000).toLocaleTimeString()} – ${new Date(vp.end*1000).toLocaleTimeString()}` : (liveMode ? `Live: last ${Math.round(vp.seconds/60*10)/10} minutes` : `${new Date(vp.start*1000).toLocaleTimeString()} – ${new Date(vp.end*1000).toLocaleTimeString()}`)}
function renderConversations(){const body=$('conversationRows');if(!body)return;const rows=filteredConversations().slice(0,200);const allCount=conversations.length;const filteredCount=rows.length;const filterTxt=egressFilter?` on ${egressFilter}`:'';$('conversationCount').textContent=rows.length?`${filteredCount} active conversations${egressFilter?` of ${allCount}`:''}${filterTxt}, sorted by current bandwidth`:(egressFilter?`No active conversations on ${egressFilter}`:'No active conversation deltas yet');body.innerHTML=rows.map(c=>{const checked=!hiddenSeries.has(c.label);const sname=c.sourceName||'';const dname=c.destinationName||'';return `<tr class="${checked?'':'mutedRow'}"><td><input type="checkbox" data-label="${escAttr(c.label)}" ${checked?'checked':''}></td><td><span class="sw" style="background:${colorForKey(c.label)}"></span>${esc(sname||'—')}</td><td>${esc(c.sourceIp||'unknown')}</td><td>${esc(dname||'—')}</td><td>${esc(c.destinationIp||'unknown')}</td><td>${esc(c.destinationPort||'')}</td><td class="num">${fmtMbpsFromBps(c.bps)}</td><td>${esc(c.egressInterface||'unknown')}</td></tr>`}).join('');body.querySelectorAll('input[type=checkbox]').forEach(cb=>{cb.onchange=()=>{const label=cb.getAttribute('data-label');if(cb.checked)hiddenSeries.delete(label);else hiddenSeries.add(label);drawAll();renderConversations();}})}

function onChartMove(ev){
  if(!chartGeom) return hideTooltip();
  const {W,H,pad,vp,keys,points,max}=chartGeom; const r=chartEl.getBoundingClientRect();
  const sx=(ev.clientX-r.left)/r.width*W, sy=(ev.clientY-r.top)/r.height*H;
  if(sx<pad.l || sx>W-pad.r || sy<pad.t || sy>H-pad.b) return hideTooltip();
  const t=vp.start+((sx-pad.l)/(W-pad.l-pad.r))*chartGeom.vp.seconds;
  let row=points[0], best=Infinity; for(const p of points){const d=Math.abs(p.t-t); if(d<best){best=d; row=p;}}
  if(!row || best>30) return hideTooltip();
  const yVal=(1-(sy-pad.t)/(H-pad.t-pad.b))*max; let hit=null;
  for(const k of keys){const v=row.vals[k]; if(v && yVal>=v[0] && yVal<=v[1] && v[2]>0){hit={key:k,bps:v[2]}; break;}}
  if(!hit) return hideTooltip();
  const parts=parseConversation(hit.key);
  showTooltip(ev, `<b>${esc(hit.key)}</b><br><span>${new Date(row.t*1000).toLocaleTimeString()}</span><br>Bandwidth: <b>${fmtMbpsFromBps(hit.bps)}</b>${parts?`<br>Source: ${esc(parts.src)}<br>Destination: ${esc(parts.dst)}<br>Port: ${esc(parts.port)}`:''}`);
}
function parseConversation(label){const m=String(label).match(/^(.*?)\s+→\s+(.*?):(\d+)(?:\s+(\w+))?$/);return m?{src:m[1],dst:m[2],port:m[3],proto:m[4]||''}:null}
function showTooltip(ev, html){const t=$('tooltip');t.innerHTML=html;t.hidden=false;const pad=14;t.style.left=(ev.clientX+pad)+'px';t.style.top=(ev.clientY+pad)+'px'}
function hideTooltip(){const t=$('tooltip'); if(t) t.hidden=true}

function miniTimeFromEvent(ev){const W=chartWidth(miniEl),p={l:72,r:24}; const r=miniEl.getBoundingClientRect(); const sx=(ev.clientX-r.left)/Math.max(1,r.width)*W; const minT=earliestTs(), maxT=latestTs(), span=Math.max(1,maxT-minT); const frac=Math.max(0,Math.min(1,(sx-p.l)/(W-p.l-p.r))); return minT+frac*span}
function onMiniDown(ev){if(chartBuckets().length<2)return; const t=miniTimeFromEvent(ev); const vp=viewport(); brushDrag={startX:ev.clientX,startOffset:viewportOffset}; if(!(t>=vp.start&&t<=vp.end)) centerViewportAt(t); ev.preventDefault()}
function onMiniMove(ev){if(!brushDrag)return; const minT=earliestTs(), maxT=latestTs(), span=Math.max(1,maxT-minT); const r=miniEl.getBoundingClientRect(); const dx=ev.clientX-brushDrag.startX; const dt=dx/r.width*span; liveMode=false; viewportOffset=Math.min(maxOffset(),Math.max(0,brushDrag.startOffset-dt)); if(viewportOffset===0) liveMode=true; drawAll()}
function onMiniUp(){brushDrag=null}
function centerViewportAt(t){const latest=latestTs(); const seconds=viewportSeconds(); const end=Math.min(latest,Math.max(earliestTs()+seconds,t+seconds/2)); liveMode=false; viewportOffset=Math.min(maxOffset(),Math.max(0,latest-end)); if(viewportOffset===0) liveMode=true; drawAll()}

function chartWidth(svg){return Math.max(1000, Math.round(svg.getBoundingClientRect().width || svg.clientWidth || 1000))}
function setSvgBox(svg,W,H){svg.setAttribute("viewBox","0 0 "+W+" "+H);svg.setAttribute("preserveAspectRatio","xMinYMin meet")}
function colorForKey(k){let h=0;for(let i=0;i<String(k).length;i++)h=(h*31+String(k).charCodeAt(i))>>>0;return colors[h%colors.length]}
function grid(svg,W,H,p){for(let i=0;i<=4;i++){const y=p.t+i*(H-p.t-p.b)/4;line(svg,p.l,y,W-p.r,y,'gridline')}line(svg,p.l,p.t,p.l,H-p.b,'axis');line(svg,p.l,H-p.b,W-p.r,H-p.b,'axis')}
function miniGrid(svg,W,H,p){line(svg,p.l,H-p.b,W-p.r,H-p.b,'axis');line(svg,p.l,p.t,p.l,H-p.b,'axis')}
function axisLabels(svg,W,H,p,max,vp){for(let i=0;i<=4;i++){const v=max*(1-i/4)/1e6;txt(svg,8,p.t+i*(H-p.t-p.b)/4+4,v.toFixed(v>=10?0:1)+' Mbps','12px');}txt(svg,p.l,H-10,new Date(vp.start*1000).toLocaleTimeString(),'12px');txt(svg,W-160,H-10,new Date(vp.end*1000).toLocaleTimeString(),'12px')}
function miniAxis(svg,W,H,p,minT,maxT){txt(svg,p.l,H-6,new Date(minT*1000).toLocaleTimeString(),'11px');txt(svg,W-145,H-6,new Date(maxT*1000).toLocaleTimeString(),'11px')}
function path(svg,d,c){const e=document.createElementNS('http://www.w3.org/2000/svg','path');e.setAttribute('d',d);e.setAttribute('fill',c);e.setAttribute('class','area');svg.appendChild(e)}
function strokePath(svg,d,cls){const e=document.createElementNS('http://www.w3.org/2000/svg','path');e.setAttribute('d',d);e.setAttribute('class',cls);svg.appendChild(e)}
function rect(svg,x,y,w,h,cls){const e=document.createElementNS('http://www.w3.org/2000/svg','rect');e.setAttribute('x',x);e.setAttribute('y',y);e.setAttribute('width',w);e.setAttribute('height',h);e.setAttribute('class',cls);svg.appendChild(e)}
function line(svg,x1,y1,x2,y2,cls){const e=document.createElementNS('http://www.w3.org/2000/svg','line');e.setAttribute('x1',x1);e.setAttribute('y1',y1);e.setAttribute('x2',x2);e.setAttribute('y2',y2);e.setAttribute('class',cls);svg.appendChild(e)}
function txt(svg,x,y,s,size){const e=document.createElementNS('http://www.w3.org/2000/svg','text');e.setAttribute('x',x);e.setAttribute('y',y);e.setAttribute('fill','#8fa1bb');e.setAttribute('font-size',size);e.textContent=s;svg.appendChild(e)}
function esc(s){return String(s).replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]))}
function escAttr(s){return esc(s).replace(/"/g,'&quot;')}
setInterval(()=>{if(liveMode && !pausedCharts)drawAll()},1000);
window.addEventListener('resize',()=>{drawAll()});
updatePauseButton();
load();connect();
