'use strict';
/* NGXDB Admin Suite — core (zero dependencies) */
const API_BASE = '/func/';
const SYS = { systemid: 100, name: '维护系统' };

const Store = {
  get: function(k){ try { return localStorage.getItem(k); } catch(e){ return null; } },
  set: function(k,v){ try { localStorage.setItem(k,v); } catch(e){} },
  del: function(k){ try { localStorage.removeItem(k); } catch(e){} },
  clear: function(){ try { localStorage.clear(); } catch(e){} }
};

const $ = function(s,r){ return (r||document).querySelector(s); };
const $$ = function(s,r){ return Array.prototype.slice.call((r||document).querySelectorAll(s)); };
function esc(s){ if(s===null||s===undefined) return ''; return String(s).replace(/[&<>"']/g,function(c){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]; }); }
function isNil(v){ return v===null||v===undefined||v===''; }
function obj2form(data){ var d={}; Object.keys(data||{}).forEach(function(k){ var v=data[k]; if(v!==undefined&&v!==null&&v!=='') d[k]=v; }); return d; }

const ICONS = {
search:'<circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>',
plus:'<path d="M12 5v14M5 12h14"/>',
edit:'<path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 013 3L7 19l-4 1 1-4z"/>',
trash:'<path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M6 6l1 14h10l1-14"/>',
restore:'<path d="M3 12a9 9 0 109-9 9 9 0 00-7 3.3L3 8"/><path d="M3 3v5h5"/>',
refresh:'<path d="M21 12a9 9 0 11-3-6.7L21 8"/><path d="M21 3v5h-5"/>',
download:'<path d="M12 3v12"/><path d="M7 11l5 5 5-5"/><path d="M4 21h16"/>',
columns:'<rect x="3" y="3" width="18" height="18" rx="2"/><path d="M9 3v18M15 3v18"/>',
close:'<path d="M6 6l12 12M18 6L6 18"/>',
chev:'<path d="M6 9l6 6 6-6"/>',
check:'<path d="M20 6L9 17l-5-5"/>',
x:'<path d="M6 6l12 12M18 6L6 18"/>',
warn:'<path d="M12 3l9 16H3z"/><path d="M12 10v4M12 17h.01"/>',
info:'<circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01"/>',
lock:'<rect x="4" y="10" width="16" height="11" rx="2"/><path d="M8 10V7a4 4 0 018 0v3"/>',
user:'<circle cx="12" cy="8" r="4"/><path d="M4 21c0-4 4-6 8-6s8 2 8 6"/>',
logout:'<path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4"/><path d="M10 17l5-5-5-5"/><path d="M15 12H3"/>',
moon:'<path d="M21 12.8A9 9 0 1111.2 3a7 7 0 009.8 9.8z"/>',
sun:'<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4 12H2M22 12h-2M5 5l1.5 1.5M17.5 17.5L19 19M19 5l-1.5 1.5M6.5 17.5L5 19"/>',
home:'<path d="M3 11l9-8 9 8"/><path d="M5 10v10h14V10"/>',
shield:'<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/>',
settings:'<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.6 1.6 0 00.3 1.8l.2.2a2 2 0 11-2.8 2.8l-.2-.2a1.6 1.6 0 00-2.7 1.1 2 2 0 11-4 0 1.6 1.6 0 00-2.7-1.1l-.2.2a2 2 0 11-2.8-2.8l.2-.2A1.6 1.6 0 003 14a2 2 0 110-4 1.6 1.6 0 001.1-2.7l-.2-.2a2 2 0 112.8-2.8l.2.2A1.6 1.6 0 0010 3a2 2 0 114 0 1.6 1.6 0 002.7 1.1l.2-.2a2 2 0 112.8 2.8l-.2.2A1.6 1.6 0 0020.6 10H21a2 2 0 110 4h-.4a1.6 1.6 0 00-1.5 1.5 2 2 0 11-2.8-2.8l.2.2A1.6 1.6 0 0019.4 4.6z"/>',
users:'<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20c0-3.5 3-5.5 6.5-5.5s6.5 2 6.5 5.5"/><path d="M17 4.2a3.5 3.5 0 010 6.6M19 14.5c2 .7 3.5 2.3 3.5 4.5"/>',
list:'<path d="M8 6h13M8 12h13M8 18h13"/><circle cx="3.5" cy="6" r="1.3"/><circle cx="3.5" cy="12" r="1.3"/><circle cx="3.5" cy="18" r="1.3"/>',
code:'<path d="M9 18l-6-6 6-6M15 6l6 6-6 6"/>',
alert:'<circle cx="12" cy="12" r="9"/><path d="M12 8v4M12 16h.01"/>',
sitemap:'<rect x="9" y="3" width="6" height="5" rx="1"/><rect x="2" y="16" width="6" height="5" rx="1"/><rect x="16" y="16" width="6" height="5" rx="1"/><path d="M12 8v4M5 16v-2h14v2"/>',
tag:'<path d="M3 7v5l8 8 8-8-8-8H8z"/><circle cx="7.5" cy="8.5" r="1.2"/>',
key:'<circle cx="8" cy="15" r="4"/><path d="M11 12l9-9M17 6l2 2M14 9l2 2"/>',
map:'<path d="M9 4L3 6v14l6-2 6 2 6-2V4l-6 2z"/><path d="M9 4v14M15 6v14"/>',
pin:'<path d="M12 21s7-6 7-11a7 7 0 10-14 0c0 5 7 11 7 11z"/><circle cx="12" cy="10" r="2.5"/>',
log:'<path d="M4 4h16v16H4z"/><path d="M8 8h8M8 12h8M8 16h5"/>',
eye:'<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7z"/><circle cx="12" cy="12" r="3"/>',
folder:'<path d="M3 7a2 2 0 012-2h4l2 2h8a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2z"/>',
folderOpen:'<path d="M3 7a2 2 0 012-2h4l2 2h8a2 2 0 012 2v1H7.2a2 2 0 00-1.9 1.4L3 18z"/><path d="M3.6 19l2.6-7.8a1.6 1.6 0 011.5-1.1H22l-2.6 7.8a1.6 1.6 0 01-1.5 1.1z"/>',
cher:'<path d="M9 6l6 6-6 6"/>'
};
function icon(n,s){s=s||16;return '<svg width="'+s+'" height="'+s+'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'+(ICONS[n]||'')+'</svg>';}

/* ---------- API ---------- */
/* 退出登录 / 会话失效：清空登录态并跳回登录页；同时用 redirect 参数记录
 * 当前页面，登录成功后原路返回。
 * 注意：这里【不使用】一次性锁。原实现用 `if(_loggingOut) return;` 会导致
 * 只要之前调用过一次 logout（例如某次会话失效），之后的“退出登录”会被
 * 静默忽略——这正是“退出登录不成功”的原因。 */
function currentPath(){
  try{ return location.pathname+location.search+location.hash; }catch(e){ return 'index.html'; }
}
function logout(msg){
  try{ Store.del('token'); Store.del('actions'); Store.del('operator'); }catch(e){}
  var url='login.html';
  try{ url+='?redirect='+encodeURIComponent(currentPath()); }catch(e){}
  if(msg){ try{ alert(msg); }catch(e){} }
  try{ location.replace(url); }catch(e){ location.href=url; }
}
async function api(path,data,opt){
  opt=opt||{};
  /* ✅ nginx postgres 模块“请求体未读全就处理”的缺陷已在 C 层修复
     （见 postgres/ngx_http_postgres_module.c 与 rebuild_nginx.sh），POST body
     现可被可靠读取，故恢复使用 POST：
       · 口令等参数不再出现在 URL / access.log（更安全）；
       · 不受 URL 长度限制（支持较大的权限 JSON）。
     同时对鉴权类错误保留退避重试，抵御偶发抖动。 */
  var tries=(opt.retries!=null)?opt.retries:2;
  var AUTH_ERR={100005:1,100006:1,100011:1,100012:1};
  for(var a=0;a<=tries;a++){
    let res;
    var __t0=(window.performance&&performance.now)?performance.now():Date.now();
    try{
      res=await fetch(API_BASE+path,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'},body:toFormData(obj2form(data||{}))});
    }catch(e){throw{net:true,message:'无法连接服务器，请稍后重试'};}
    /* 性能打点：打开浏览器开发者工具(F12)→Console，可看到每次接口的
       “发起→收到响应”耗时（不含页面渲染）。
       · 若这里就 >300ms：慢在 8080 代理/网络链路（服务器端实测仅 10~22ms）；
       · 若这里 <50ms 而页面仍觉得慢：慢在浏览器渲染（虚拟机软件渲染）。 */
    try{ var __dt=((window.performance&&performance.now)?performance.now():Date.now())-__t0;
         console.debug('[ngxdb] '+path+' → '+__dt.toFixed(0)+'ms'); }catch(e){}
    let j;
    try{j=await res.json();}catch(e){throw{net:true,message:'服务器返回了无法解析的内容'};}
    if(AUTH_ERR[j.errorcode]){
      if(a<tries){ await new Promise(function(r){setTimeout(r,300);}); continue; }
      logout('登录已超时，请重新登录');throw{handled:true,message:j.message};
    }
    if(j.errorcode!==0&&!opt.allowError){throw{code:j.errorcode,message:j.message||('错误代码 '+j.errorcode)};}
    return j;
  }
}
function toFormData(o){var b=new URLSearchParams();Object.keys(o||{}).forEach(function(k){var v=o[k];if(v!==undefined&&v!==null&&v!=='')b.append(k,v);});return b;}

/* ---------- toast ---------- */
function toast(msg,type){
  type=type||'info';
  let wrap=$('.toast-wrap');
  if(!wrap){wrap=document.createElement('div');wrap.className='toast-wrap';document.body.appendChild(wrap);}
  var t=document.createElement('div');
  t.className='toast toast--'+(type==='ok'?'ok':type==='err'?'err':type==='warn'?'warn':'info');
  t.innerHTML=icon(type==='ok'?'check':type==='err'?'x':type==='warn'?'warn':'info',18)+'<div>'+esc(msg)+'</div>';
  wrap.appendChild(t);
  setTimeout(function(){t.style.transition='opacity .25s,transform .25s';t.style.opacity='0';t.style.transform='translateX(30px)';setTimeout(function(){t.remove()},260);},3200);
}

/* ---------- modal ---------- */
const Modal = {
  el:null,
  open: function(o){
    this.close();
    var m=document.createElement('div');
    m.className='modal open';
    m.innerHTML='<div class="modal__mask"></div><div class="modal__box'+(o.wide?' wide':'')+'\">'+
      '<div class="modal__head">'+(o.icon?icon(o.icon,18):'')+'<h3>'+esc(o.title||'')+'</h3>'+
      '<button class="icon-btn" data-close>'+icon('close',18)+'</button></div>'+
      '<div class="modal__body">'+(o.body||'')+'</div>'+
      (o.footer===false?'':'<div class="modal__foot">'+(o.footer||'')+'</div>')+'</div>';
    document.body.appendChild(m);
    this.el=m;
    m.querySelector('.modal__mask').addEventListener('click',function(){ if(o.maskClose===false) return; Modal.close(); });
    $$('[data-close]',m).forEach(function(b){b.addEventListener('click',function(){Modal.close();});});
    if(o.onMount) o.onMount(m);
    return m;
  },
  close: function(){ if(this.el){this.el.remove();this.el=null;} }
};

/* ---------- confirm ---------- */
function confirmBox(msg,okText){
  return new Promise(function(resolve){
    var m=Modal.open({title:'请确认',icon:'warn',
      body:'<div style="font-size:13.5px;line-height:1.7">'+esc(msg)+'</div>',
      footer:'<button class="btn" data-c>取消</button><button class="btn btn--primary" data-ok>'+esc(okText||'确定')+'</button>'});
    $('[data-c]',m).addEventListener('click',function(){Modal.close();resolve(false);});
    $('[data-ok]',m).addEventListener('click',function(){Modal.close();resolve(true);});
  });
}

