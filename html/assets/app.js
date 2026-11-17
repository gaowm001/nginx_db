'use strict';
/* NGXDB Admin — SPA 主逻辑（登录态 / 路由 / 侧边栏 / 表格 / CRUD / 权限显隐）
 * 数据模型对齐真实后端：
 *   查询  base+'_query'  仅传 token（后端返回全量 rows）
 *   新增  base+'_add'    token + 业务字段
 *   修改  base+'_edit'   token + 业务字段 + 主键(pk)
 *   删除  base+'_del'    token + 主键(pk)
 *   恢复  base+'_undel'  token + 主键(pk)
 * 权限：菜单/按钮 actionid ∈ localStorage.actions 才显示
 */
var TOKEN=Store.get('token')||'';
var ACTIONS=String(Store.get('actions')||'').split(',').filter(Boolean);
var OPERATOR={};try{OPERATOR=JSON.parse(Store.get('operator')||'{}');}catch(e){}
var THEME=Store.get('theme')||'light';
var _state={m:null,rows:[],pageRows:[],page:1,ps:15,sel:null,q:'',collapsed:{}};

var FIELD_CN={systemid:'系统ID',systemname:'系统名称',loginname:'登录名',prikey:'密钥',algorithm:'算法',
 isused:'启用',createtime:'创建时间',updatetime:'更新时间',deltime:'删除时间',createoperator:'创建人',updateoperator:'更新人',deloperator:'删除人',
 operatorid:'员工ID',operatorname:'员工姓名',accounts:'账号',tokeninterval:'令牌间隔',tokentype:'令牌类型',phone:'电话',sex:'性别',nickname:'昵称',birthday:'生日',memo:'备注',mycode:'编码',upcode:'上级编码',headimgurl:'头像',
 sysactionid:'权限ID',sysactionname:'权限名',upid:'上级ID',upname:'上级',actionid:'动作ID',params:'参数',isdefault:'默认',actionname:'名称',actionurl:'接口',code:'编码',
 orgtypeid:'类型ID',orgtypename:'类型名',description:'描述',
 sysorgid:'部门ID',sysorgname:'部门名',orgtype:'类型',orgtypename:'类型名',
 roleinfoid:'角色ID',roleinfoname:'角色名',
 logid:'日志ID',logtime:'时间',clientip:'客户端IP',serverip:'服务器IP',funcname:'功能',pckname:'包名',res:'结果',content:'内容',
 errorcode:'错误码',message:'信息',primekey:'主键',schema:'模式',
 pid:'省ID',province:'省份',cid:'市ID',city:'城市'};

function hasPerm(id){return id!=null&&ACTIONS.indexOf(String(id))>=0;}
function byId(id){for(var i=0;i<MODULES.length;i++)if(MODULES[i].id===id)return MODULES[i];return null;}
function cn(f){return FIELD_CN[f]||f;}
function groupName(g){for(var i=0;i<GROUPS.length;i++)if(GROUPS[i].id===g)return GROUPS[i].t;return '';}
function isOn(v){return v===1||v==='1'||v===true||v==='true'||v==='启用';}
function findRow(m,pk){for(var i=0;i<_state.rows.length;i++)if(String(_state.rows[i][m.pk])===String(pk))return _state.rows[i];return null;}
function boolBadge(v){
  if(isOn(v))return '<span class="badge badge--on"><span class="dot"></span>启用</span>';
  if(v===null||v===undefined||v==='')return '<span class="badge badge--mute">—</span>';
  return '<span class="badge badge--off"><span class="dot"></span>停用</span>';
}
function statCard(ic,label,val){return '<div class="stat"><div class="stat__label">'+icon(ic,15)+esc(label)+'</div><div class="stat__value">'+esc(val)+'</div></div>';}

/* ============ 启动 ============ */
(function init(){
  applyTheme();
  if(!TOKEN){logout();return;}
  try{$('#brand-logo').innerHTML=icon('shield',20);}catch(e){}
  try{$('#burger').innerHTML='<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M3 6h18M3 12h18M3 18h18"/></svg>';}
  catch(e){}
  renderTopbar();
  renderSidebar();
  bindGlobal();
  window.addEventListener('hashchange',route);
  if(!location.hash||location.hash==='#'){location.replace('#/'+defaultModule());}
  route();
})();

function applyTheme(){try{document.documentElement.setAttribute('data-theme',THEME);}catch(e){}}

function defaultModule(){for(var i=0;i<MODULES.length;i++)if(hasPerm(MODULES[i].menu))return MODULES[i].id;return MODULES.length?MODULES[0].id:'';}

/* ============ 顶栏 ============ */
function renderTopbar(){
  var name=OPERATOR.operatorname||'管理员';
  var org=OPERATOR.systemname||SYS.name;
  var av=$('#avatar');if(av)av.textContent=(name||'A').slice(0,1);
  var n=$('#uc-name');if(n)n.textContent=name;
  var o=$('#uc-org');if(o)o.textContent=org;
  var mp=$('#menu-profile');if(mp)mp.innerHTML=icon('user',16)+'<span>当前账号</span>';
  var ml=$('#menu-logout');if(ml)ml.innerHTML=icon('logout',16)+'<span>退出登录</span>';
  var mpass=$('#menu-pass');
  if(mpass){if(hasPerm('141')){mpass.innerHTML=icon('lock',16)+'<span>修改密码</span>';}else{mpass.style.display='none';}}
}

function bindGlobal(){
  var bt=$('#btn-theme');
  if(bt){bt.innerHTML=icon(THEME==='dark'?'sun':'moon',18);bt.addEventListener('click',function(){
    THEME=THEME==='dark'?'light':'dark';Store.set('theme',THEME);applyTheme();bt.innerHTML=icon(THEME==='dark'?'sun':'moon',18);});}
  var uc=$('#userchip');
  if(uc){var menu=$('#user-menu');
    uc.addEventListener('click',function(e){e.stopPropagation();menu.classList.toggle('open');});
    document.addEventListener('click',function(){menu.classList.remove('open');});}
  var mo=$('#menu-logout');if(mo)mo.addEventListener('click',function(){logout();});
  var mpb=$('#menu-pass');if(mpb)mpb.addEventListener('click',function(){openPassModal();});
  var bg=$('#burger');if(bg)bg.addEventListener('click',function(){$('#app').classList.toggle('nav-open');});
  var bd=$('#backdrop');if(bd)bd.addEventListener('click',function(){$('#app').classList.remove('nav-open');});
}

/* ============ 侧边栏 ============ */
function renderSidebar(){
  var nav=$('#sidebar-nav');if(!nav)return;
  var sq=null;MODULES.forEach(function(m){if(m.base==='sysinfo/sysaction')sq=m;});
  if(!sq||!TOKEN){renderSidebarStatic();return;}
  api(sq.base+'_query',{token:TOKEN}).then(function(res){
    var rows=(res.info&&res.info.rows)||[];
    if(!rows.length){renderSidebarStatic();return;}
    renderNavFromRows(rows);
  }).catch(function(){renderSidebarStatic();});
}
function renderSidebarStatic(){
  var nav=$('#sidebar-nav');if(!nav)return;
  var html='';
  GROUPS.forEach(function(g){
    if(!hasPerm(g.id))return;
    var items='';
    MODULES.forEach(function(m){
      if(m.g!==g.id)return;
      if(!hasPerm(m.menu))return;
      items+='<li class="nav-item" data-mod="'+m.id+'">'+icon(m.ic,17)+esc(m.t)+'</li>';
    });
    if(!items)return;
    html+='<div class="nav-group"><div class="nav-group__title">'+icon(g.ic,15)+'<span>'+esc(g.t)+'</span><span class="chev">'+icon('chev',16)+'</span></div>'
      +'<ul class="nav-group__list">'+items+'</ul></div>';
  });
  bindNav(nav,html);
}
/* 无对应模块页面的权限节点 → 空占位页（hash: blank-<sysactionid>），标题取节点名 */
var BLANK_TITLES={};
/* 按 sysaction 树渲染左侧菜单（仅显示当前账号有权限的节点）：
 * 指向具体函数的操作级节点（actionid 非空）表示到尾部了，不生成菜单——菜单到功能级为止
 * （如 ERP→字典）；功能节点有模块的进模块页面，没有模块页面的进空占位页。 */
function renderNavFromRows(rows){
  var nav=$('#sidebar-nav');if(!nav)return;
  var byId={};rows.forEach(function(r){byId[r.sysactionid]=r;});
  var kids={};rows.forEach(function(r){var k=r.upid;(kids[k]=kids[k]||[]).push(r);});
  Object.keys(kids).forEach(function(k){kids[k].sort(function(a,b){return a.idpath<b.idpath?-1:1;});});
  var modByMenu={};MODULES.forEach(function(m){modByMenu[m.menu]=m;});
  var visible={};
  rows.forEach(function(r){
    if(ACTIONS.indexOf(String(r.sysactionid))<0)return;
    var c=r;
    while(c&&byId[c.upid]){visible[c.sysactionid]=true;c=byId[c.upid];}
    if(c)visible[c.sysactionid]=true;
  });
  /* 节点 → 菜单 HTML：功能→导航项；目录(无模块但有子级)→可折叠子菜单，
     点击标题展开/收起下级内容；叶子且无模块→空占位页。 */
  function nodeHtml(n){
    var m=modByMenu[n.sysactionid];
    if(m)return '<li class="nav-item" data-mod="'+m.id+'" title="'+esc(n.sysactionname)+'">'+icon(m.ic,17)+esc(n.sysactionname)+'</li>';
    var kid='';
    (kids[n.sysactionid]||[]).forEach(function(c){
      if(!visible[c.sysactionid])return;
      if(c.actionid)return;   /* 指向函数：尾部操作级节点，不上菜单 */
      kid+=nodeHtml(c);
    });
    if(!kid){BLANK_TITLES['blank-'+n.sysactionid]=n.sysactionname;
      return '<li class="nav-item" data-mod="blank-'+n.sysactionid+'" title="'+esc(n.sysactionname)+'">'+icon('folder',17)+esc(n.sysactionname)+'</li>';}
    return '<li class="nav-dir"><div class="nav-dir__title">'+icon('folder',15)+'<span>'+esc(n.sysactionname)+'</span><span class="chev">'+icon('chev',14)+'</span></div><ul class="nav-dir__list">'+kid+'</ul></li>';
  }
  var html='';
  (kids[0]||[]).forEach(function(root){
    (kids[root.sysactionid]||[]).forEach(function(g){
      if(!visible[g.sysactionid])return;
      var items='';
      (kids[g.sysactionid]||[]).forEach(function(n){
        if(!visible[n.sysactionid])return;
        if(n.actionid)return;   /* 指向函数：尾部操作级节点，不上菜单 */
        items+=nodeHtml(n);
      });
      if(!items)return;
      html+='<div class="nav-group"><div class="nav-group__title"><span>'+esc(g.sysactionname)+'</span><span class="chev">'+icon('chev',16)+'</span></div><ul class="nav-group__list">'+items+'</ul></div>';
    });
  });
  if(!html){renderSidebarStatic();return;}
  bindNav(nav,html);
}
function bindNav(nav,html){
  nav.innerHTML=html;
  $$('#sidebar-nav .nav-group__title').forEach(function(t){t.addEventListener('click',function(){t.parentElement.classList.toggle('closed');});});
  $$('#sidebar-nav .nav-dir__title').forEach(function(t){t.addEventListener('click',function(){t.parentElement.classList.toggle('closed');});});
  $$('#sidebar-nav .nav-item').forEach(function(a){a.addEventListener('click',function(){location.hash='#/'+a.getAttribute('data-mod');try{$('#app').classList.remove('nav-open');}catch(e){}});});
}

/* ============ 路由 ============ */
function route(){
  var id=(location.hash||'').replace(/^#\//,'');
  var m=byId(id);
  if(!m&&/^blank-/.test(id)){renderBlankPage(id);return;}
  if(!m){id=defaultModule();location.replace('#/'+id);m=byId(id);}
  if(!m){$('#content').innerHTML='<div class="empty">无可用模块</div>';return;}
  if(!hasPerm(m.menu)){/* 权限树变更后旧登录态的 ACTIONS 不含新菜单 id：引导重新登录（记住密码时可一键返回本页） */
    logout('您对该功能的访问权限已变更，请重新登录');return;}
  $$('#sidebar-nav .nav-item').forEach(function(a){a.classList.toggle('active',a.getAttribute('data-mod')===m.id);});
  if(m.id==='gethelp'){renderGetHelp(m);return;}
  renderModule(m);
}
/* 空占位页面：权限树上已配置、但尚无对应模块页面的节点 */
function renderBlankPage(id){
  _state.m=null;_state.page=1;_state.sel=null;_state.q='';_state.collapsed={};_state.node=null;_state.expanded={};_state.hiddenCols=[];_state.view='table';_state.filtered=[];
  var t=BLANK_TITLES[id]||'功能页面';
  var tt=$('#topbar-title');if(tt)tt.textContent=t;
  $$('#sidebar-nav .nav-item').forEach(function(a){a.classList.toggle('active',a.getAttribute('data-mod')===id);});
  $('#content').innerHTML='<div class="card"><div class="card__head">'+icon('folder',16)+'<h3>'+esc(t)+'</h3></div>'
    +'<div class="empty" style="padding:60px 20px">'+icon('pen',32)+'<p>该功能页面暂未提供</p></div></div>';
}

/* ============ 渲染模块 ============ */
function renderModule(m){
  _state.m=m;_state.page=1;_state.sel=null;_state.q='';_state.collapsed={};_state.node=null;_state.expanded={};_state.hiddenCols=[];_state.view='table';_state.filtered=[];
  var tt=$('#topbar-title');if(tt)tt.textContent=m.t;
  var canAdd=m.fields&&m.fields.length&&m.ops.add&&hasPerm(m.ops.add);
  var tb='<div class="toolbar">'
    +(canAdd?'<button class="btn btn--primary" data-tb="add">'+icon('plus',16)+'新增</button>':'')
    +(m.ops.edit&&hasPerm(m.ops.edit)?'<button class="btn" data-tb="edit">'+icon('edit',16)+'编辑</button>':'')
    +(m.ops.del&&hasPerm(m.ops.del)?'<button class="btn btn--danger" data-tb="del">'+icon('trash',16)+'删除</button>':'')
    +(m.ops.undel&&hasPerm(m.ops.undel)?'<button class="btn" data-tb="undel">'+icon('restore',16)+'恢复</button>':'')
    +(m.dt?'<button class="btn" data-tb="detail">'+icon('eye',16)+'权限</button>':'')
    +'<div class="toolbar__spacer"></div>'
    +'<button class="btn btn--icon" data-tb="view" title="表格/卡片视图切换">'+icon('list',18)+'</button>'
    +'<button class="btn btn--icon" data-tb="colsel" title="显示列">'+icon('columns',18)+'</button>'
    +'<button class="btn btn--icon" data-tb="export" title="导出">'+icon('download',18)+'</button>'
    +'<div class="search">'+icon('search',16)+'<input class="input" id="search-in" placeholder="模糊搜索…"></div>'
    +'<button class="btn btn--icon" data-tb="refresh" title="刷新">'+icon('refresh',18)+'</button>'
    +'</div>';
  var c=$('#content');
  c.innerHTML='<div class="page-head"><div><h1>'+icon(m.ic,22)+' '+esc(m.t)+'</h1>'
    +'<div class="crumb">NGXDB<i>/</i>'+esc(groupName(m.g))+'<i>/</i>'+esc(m.t)+'</div></div></div>'
    +'<div class="stats" id="stats"></div>'
    +'<div class="card">'+tb
    +(m.tree
      ?'<div class="explorer"><div class="tpane" id="treePane"></div><div class="lpane"><div class="crumbs" id="crumbs"></div><div class="table-wrap" id="tbl"></div><div class="pager" id="pager"></div></div></div>'
      :'<div class="table-wrap" id="tbl"></div><div class="pager" id="pager"></div>')
    +'</div>';
  bindToolbar(m);
  loadList(m);
}

/* ===== 接口文档：展示数据库全部函数元数据（sysinfo/help_query，权限 9113；注释编辑走 help_edit 权限 224） ===== */
function renderGetHelp(m){
  _state.m=m;_state.q='';_state.rows=[];_state.filtered=[];_state.page=1;
  var tt=$('#topbar-title');if(tt)tt.textContent=m.t;
  $('#content').innerHTML='<div class="page-head"><div><h1>'+icon(m.ic,22)+' '+esc(m.t)+'</h1>'
    +'<div class="crumb">NGXDB<i>/</i>'+esc(groupName(m.g))+'<i>/</i>'+esc(m.t)+'</div></div></div>'
    +'<div class="stats" id="stats"></div>'
    +'<div class="card"><div class="toolbar">'
      +'<div class="search">'+icon('search',16)+'<input class="input" id="search-in" placeholder="模糊搜索：模式 / 函数名 / 参数 / 返回类型 / 说明…"></div>'
      +'<div class="toolbar__spacer"></div>'
      +'<button class="btn btn--icon" id="api-refresh" title="刷新">'+icon('refresh',18)+'</button>'
    +'</div><div class="table-wrap" id="tbl"></div><div class="pager" id="pager"></div></div>';
  $('#api-refresh').addEventListener('click',function(){loadGetHelp(m);});
  $('#search-in').addEventListener('input',function(){_state.q=this.value;_state.page=1;renderApiRows();});
  loadGetHelp(m);
}
async function loadGetHelp(m){
  var tbl=$('#tbl');if(!tbl)return;
  tbl.innerHTML='<div class="empty"><div class="spinner"></div></div>';
  try{
    var j=await api(m.base,{token:TOKEN});
    _state.rows=(j.info&&j.info.rows)||[];
    renderApiRows();
  }catch(e){
    _state.rows=[];_state.filtered=[];
    tbl.innerHTML='<div class="empty">'+icon('alert',36)+'<div>'+esc(e.message||'加载失败')+'</div></div>';
    var st=$('#stats');if(st)st.innerHTML='';
    var pg=$('#pager');if(pg)pg.innerHTML='';
    if(e.net)toast(e.message,'err');
  }
}
function renderApiRows(){
  var rows=_state.rows,q=(_state.q||'').toLowerCase();
  var fs=['schemaname','funcname','arguments','returntype','description'];
  var filtered=q?rows.filter(function(r){return fs.some(function(f){var v=r[f];return v!=null&&String(v).toLowerCase().indexOf(q)>=0;});}):rows;
  _state.filtered=filtered;
  var ps=_state.ps,pages=Math.max(1,Math.ceil(filtered.length/ps));
  if(_state.page>pages)_state.page=pages;
  var s=(_state.page-1)*ps,pageRows=filtered.slice(s,s+ps);
  var st=$('#stats');if(st)st.innerHTML=statCard('list','函数总数',rows.length)+statCard('code','当前显示',filtered.length);
  var canEdit=hasPerm(224);
  var head='<tr><th>模式</th><th>函数名</th><th>参数</th><th>返回类型</th><th>说明</th>'+(canEdit?'<th style="text-align:right">操作</th>':'')+'</tr>';
  var body=pageRows.length?pageRows.map(function(r){
    return '<tr><td>'+esc(r.schemaname||'')+'</td>'
      +'<td><b>'+esc(r.funcname||'')+'</b></td>'
      +'<td class="nowrap-el" title="'+esc(r.arguments||'')+'">'+esc(r.arguments||'—')+'</td>'
      +'<td>'+esc(r.returntype||'')+'</td>'
      +'<td class="nowrap-el" title="'+esc(r.description||'')+'">'+esc(r.description||'—')+'</td>'
      +(canEdit?'<td style="text-align:right;white-space:nowrap"><button class="btn btn--sm btn--ghost btn--icon" data-editdesc="1" data-schema="'+esc(r.schemaname||'')+'" data-func="'+esc(r.funcname||'')+'" data-args="'+esc(r.arguments||'')+'" data-desc="'+esc(r.description||'')+'" title="修改注释">'+icon('edit',16)+'</button></td>':'')+'</tr>';
  }).join(''):'<tr><td colspan="'+(5+(canEdit?1:0))+'"><div class="empty">'+icon('list',34)+'<div>'+(q?'无匹配结果':'暂无数据')+'</div></div></td></tr>';
  $('#tbl').innerHTML='<table class="data"><thead>'+head+'</thead><tbody>'+body+'</tbody></table>';
  $$('#tbl [data-editdesc]').forEach(function(b){
    b.addEventListener('click',function(){
      openHelpEdit(b.getAttribute('data-schema'),b.getAttribute('data-func'),b.getAttribute('data-args'),b.getAttribute('data-desc'));
    });
  });
  /* 分页（结构与通用 renderPager 一致，点击回调走 renderApiRows） */
  var p=$('#pager');
  if(p){
    var btns='<button data-pg="prev" '+(_state.page<=1?'disabled':'')+'>上一页</button>';
    var dots=false;
    for(var i=1;i<=pages;i++){
      if(pages>7&&i>2&&i<pages-1&&Math.abs(i-_state.page)>1){
        if(!dots){btns+='<button disabled>…</button>';dots=true;}
        continue;
      }
      dots=false;
      btns+='<button data-pg="'+i+'" class="'+(i===_state.page?'active':'')+'">'+i+'</button>';
    }
    btns+='<button data-pg="next" '+(_state.page>=pages?'disabled':'')+'>下一页</button>';
    p.innerHTML='<div class="pager__info">共 '+filtered.length+' 条</div>'+btns;
    $$('#pager button[data-pg]').forEach(function(b){
      b.addEventListener('click',function(){
        var v=b.getAttribute('data-pg');
        if(v==='prev')_state.page--;else if(v==='next')_state.page++;else _state.page=+v;
        renderApiRows();
      });
    });
  }
}

/* 修改接口函数注释（sysinfo/help_edit，权限 224）：保存到 pg_description */
function openHelpEdit(schema,func,args,desc){
  var md=Modal.open({title:'修改接口注释 — '+schema+'.'+func,icon:'edit',
    body:'<div class="field"><label>说明（同步到函数 COMMENT，接口文档实时显示）</label><textarea class="textarea" id="help-desc" rows="5">'+esc(desc||'')+'</textarea></div>',
    footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button><button class="btn btn--primary" data-ok>'+icon('check',16)+'保存</button>'});
  md.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  md.querySelector('[data-ok]').addEventListener('click',async function(){
    var d=$('#help-desc',md).value;
    try{
      await api('sysinfo/help_edit',{token:TOKEN,schemaname:schema,funcname:func,description:d,arguments:args});
      toast('注释已保存','ok');Modal.close();
      if(_state.m&&_state.m.id==='gethelp')loadGetHelp(_state.m);
    }catch(e){ if(!e.handled)toast(e.message||'保存失败','err'); }
  });
  setTimeout(function(){var f=$('#help-desc',md);if(f)f.focus();},60);
}

function bindToolbar(m){
  $$('#content [data-tb]').forEach(function(b){
    b.addEventListener('click',function(){
      var a=b.getAttribute('data-tb');
      if(a==='add')openForm(m,null);
      else if(a==='edit'){var r=needRow(m);if(r)openForm(m,r);}
      else if(a==='del'){var r2=needRow(m);if(r2)doDel(m,r2,'del');}
      else if(a==='undel'){var r3=needRow(m);if(r3)doDel(m,r3,'undel');}
      else if(a==='detail'){var r4=needRow(m);if(r4)openDetail(m,r4);}
      else if(a==='refresh')loadList(m);
      else if(a==='view'){_state.view=(_state.view==='card')?'table':'card';renderTable(m);}
      else if(a==='colsel')openColPicker(m);
      else if(a==='export')openExportMenu(m);
    });
  });
  var si=$('#search-in');
  if(si)si.addEventListener('input',function(){_state.q=si.value;_state.page=1;renderTable(m);});
}
function needRow(m){if(!_state.sel){toast('请先在表格中点击选择一行','warn');return null;}return _state.sel;}

/* ============ 查询 ============ */
async function loadList(m){
  var wrap=$('#tbl');if(!wrap)return;
  wrap.innerHTML='<div class="spinner"></div>';
  $('#pager').innerHTML='';var st=$('#stats');if(st)st.innerHTML='';
  try{
    /* loginlog 页 base 已是完整函数名(loginlog_query)，其余模块 base+'_query' */
    var url=(m.id==='loginlog')?m.base:(m.base+'_query');
    var params={token:TOKEN};
    /* serverlog 日志量随时间增长：走后端分页（rows/page），避免全量拉取导致响应过大 */
    if(m.id==='serverlog'){params.rows=20;params.page=Math.max(1,_state.page||1);}
    var j=await api(url,params);
    _state.rows=(j.info&&j.info.rows)||[];
    _state.total=(j.info&&j.info.total)||_state.rows.length;
    _state.serverPaged=(m.id==='serverlog');
    if(!params.rows)_state.page=1;
    renderTable(m);
  }catch(e){
    _state.rows=[];
    wrap.innerHTML='<div class="empty">'+icon('alert',36)+'<div>'+(esc(e.message||'加载失败'))+'</div></div>';
    var tp=$('#treePane');if(tp)tp.innerHTML='';
    var cr=$('#crumbs');if(cr)cr.innerHTML='';
    if(e.net)toast(e.message,'err');
  }
}

function renderTable(m){
  var rows=_state.rows,q=(_state.q||'').toLowerCase();
  if(m.tree){
    var hit={},vis;
    if(q){
      /* 搜索：命中节点连同其各级父节点一起显示（idpath 为"根→自身"完整链，前缀即各级祖先） */
      rows.forEach(function(r){
        if(m.cols.some(function(c){var v=r[c.f];return v!=null&&String(v).toLowerCase().indexOf(q)>=0;})){
          var p=String(r.idpath||'');
          if(!p)hit[String(r[m.pk])]=1;
          else{var parts=p.split('.');for(var i=1;i<=parts.length;i++)hit[parts.slice(0,i).join('.')]=1;}
        }
      });
      vis=rows.filter(function(r){var p=String(r.idpath||'');return p?!!hit[p]:!!hit[String(r[m.pk])];});
    }else if(_state.node!=null){
      /* 右栏显示当前节点自身（首行，带"当前"标识）+ 其直接下级：
         根节点只出现在左树，若不在右栏展示则永远无法查看/修改根信息 */
      var pre=String(_state.node)+'.';
      var cur=null;
      for(var ci0=0;ci0<rows.length;ci0++){
        if(String(rows[ci0].idpath||'')===String(_state.node)){cur=rows[ci0];break;}
      }
      vis=rows.filter(function(r){
        var p=String(r.idpath||'');
        return p.indexOf(pre)===0&&p.indexOf('.',pre.length)<0;
      });
      if(cur)vis=[cur].concat(vis);
    }else{
      /* 未选择节点：显示顶级节点 */
      vis=rows.filter(function(r){return treeDepth(r)===0;});
    }
    _state.filtered=vis;
    renderExplorer(m,vis);
    return;
  }
  var filtered=q?rows.filter(function(r){return m.cols.some(function(c){var v=r[c.f];return v!=null&&String(v).toLowerCase().indexOf(q)>=0;});}):rows;
  _state.filtered=filtered;
  if(_state.serverPaged&&m.id==='serverlog'){
    /* 后端分页（serverlog）：_state.rows 即当前页，前端不再切片；翻页时重发请求 */
    _state.pageRows=filtered;
    var st0=$('#stats');
    if(st0)st0.innerHTML=statCard('list','记录总数',_state.total||filtered.length);
    renderRows(m,filtered);
    renderPager(_state.total||filtered.length,Math.max(1,Math.ceil((_state.total||filtered.length)/20)));
    return;
  }
  var ps=_state.ps,pages=Math.max(1,Math.ceil(filtered.length/ps));
  if(_state.page>pages)_state.page=pages;
  var s=(_state.page-1)*ps,pageRows=filtered.slice(s,s+ps);
  _state.pageRows=pageRows;
  var st=$('#stats');
  if(st){var on=0,hasUsed=m.cols.some(function(c){return c.b;});
    if(hasUsed)rows.forEach(function(r){var c=m.cols.filter(function(x){return x.b;})[0];if(c&&isOn(r[c.f]))on++;});
    st.innerHTML=statCard('list','记录总数',rows.length)+(hasUsed?statCard('check','已启用',on):'');}
  renderRows(m,pageRows);
  renderPager(filtered.length,pages);
}

/* ===== 资源管理器式两栏展示：左=层级树，右=当前层级列表 ===== */
function nameOf(m,r){
  var f=null;
  m.cols.forEach(function(c){if(!f&&c.f&&c.f.toLowerCase().indexOf('name')>=0)f=c.f;});
  if(!f)f=m.cols[0].f;
  var v=r[f];var s=(v==null)?'':String(v);
  return s.trim()?s:String(r[m.pk]!=null?r[m.pk]:'');
}
function renderExplorer(m,visible){
  /* 首次进入自动展开第一层 */
  if(!Object.keys(_state.expanded).length){
    _state.rows.forEach(function(r){if(treeDepth(r)===0)_state.expanded[String(r.idpath||r[m.pk])]=1;});
  }
  renderTree(m);
  var ps=_state.ps,pages=Math.max(1,Math.ceil(visible.length/ps));
  if(_state.page>pages)_state.page=pages;
  var s=(_state.page-1)*ps,pageRows=visible.slice(s,s+ps);
  _state.pageRows=pageRows;
  var st=$('#stats');
  if(st){var on=0,c0=null;m.cols.forEach(function(c){if(c.b&&!c0)c0=c;});
    if(c0)_state.rows.forEach(function(r){if(isOn(r[c0.f]))on++;});
    st.innerHTML=statCard('list','记录总数',_state.rows.length)+(c0?statCard('check','已启用',on):'');}
  renderCrumbs(m,visible.length);
  renderRows(m,pageRows);
  renderPager(visible.length,pages);
}
function renderCrumbs(m,count){
  var cr=$('#crumbs');if(!cr)return;
  if((_state.q||'').toLowerCase()){
    cr.innerHTML='<span class="crumb-item">'+icon('search',13)+'</span><span>搜索“'+esc(_state.q)+'” — 命中 '+count+' 项（含各级父节点）</span>';
    return;
  }
  if(_state.node==null){
    cr.innerHTML='<span class="crumb-item">'+icon('home',13)+'</span><span>全部顶级节点</span><span class="crumb-sep">/</span><span class="crumb-item muted">'+count+' 项</span>';
    return;
  }
  var byId={};_state.rows.forEach(function(r){var p=String(r.idpath||'');if(p)byId[p]=r;});
  var cur=byId[String(_state.node)];
  var segs=(cur&&cur.idpath)?String(cur.idpath).split('.'):[String(_state.node)];
  var h='<span class="crumb-item link" data-node="">'+icon('home',13)+'全部</span>';
  segs.forEach(function(sid,i){
    var pre2=segs.slice(0,i+1).join('.');
    var r=byId[pre2],nm=r?nameOf(m,r):sid;
    h+='<span class="crumb-sep">/</span><span class="crumb-item'+(i<segs.length-1?' link" data-node="'+esc(pre2):' strong')+'">'+esc(nm)+'</span>';
  });
  h+='<span class="crumb-sep">/</span><span class="crumb-item muted">'+count+' 项</span>';
  cr.innerHTML=h;
  $$('#crumbs .link').forEach(function(el){el.addEventListener('click',function(){
    var v=el.getAttribute('data-node');_state.node=(v===''||v==null)?null:v;_state.page=1;renderTable(_state.m);});});
}
/* 树面板节点统一显示：主键 | 创建时的第一个业务字段值。
   “第一个业务字段”= 生成模块时 fields 的第一个非结构列（排除主键/isused/systemid/树结构列）。 */
function firstBizField(m){
  var skip={'isused':1,'systemid':1,'upid':1,'idpath':1,'idlevel':1,'idcount':1};
  var i,f;
  for(i=0;i<(m.fields||[]).length;i++){f=m.fields[i];if(f&&f.f&&!skip[f.f])return f.f;}
  for(i=0;i<(m.cols||[]).length;i++){f=m.cols[i];if(f&&f.f&&!skip[f.f])return f.f;}
  return null;
}
function renderTree(m){
  var tp=$('#treePane');if(!tp)return;
  var rows=_state.rows,children={};
  /* idpath 为"根→自身"完整链：每个节点挂在"父 idpath"（去掉自身末段）之下，顶级挂在 '' */
  rows.forEach(function(r){
    var p=String(r.idpath||''),key='';
    if(p){var parts=p.split('.');if(parts.length>1)key=parts.slice(0,parts.length-1).join('.');}
    (children[key]||(children[key]=[])).push(r);
  });
  function nodeHtml(r,depth){
    var id=String(r.idpath||r[m.pk]),kids=children[id]||[],has=kids.length>0,open=!!_state.expanded[id];
    var fb=firstBizField(m),fv=(fb&&r[fb]!=null)?String(r[fb]).trim():'';
    var label=String(r[m.pk]!=null?r[m.pk]:'')+(fv?': '+fv:'');
    var h='<div class="tnode'+(_state.node===id?' on':'')+'" style="padding-left:'+(6+depth*16)+'px" data-id="'+esc(id)+'">'
      +'<span class="tw'+(has?'':' leaf')+'" data-toggle="'+(has?id:'')+'" title="'+(has?(open?'折叠':'展开'):'')+'">'+(has?icon(open?'chev':'cher',13):'')+'</span>'
      +(has?icon(open?'folderOpen':'folder',14):'')
      +'<span class="tlabel">'+esc(label)+'</span>'
      +(has?'<span class="cnt">'+kids.length+'</span>':'')+'</div>';
    if(has&&open)kids.forEach(function(k){h+=nodeHtml(k,depth+1);});
    return h;
  }
  var roots=children['']||[];
  var h='<div class="tpane-hd">'+icon('sitemap',14)+'层级导航</div>';
  if(!roots.length)h+='<div class="empty" style="padding:16px">暂无数据</div>';
  roots.forEach(function(r){h+=nodeHtml(r,0);});
  tp.innerHTML=h;
  $$('#treePane .tnode').forEach(function(el){el.addEventListener('click',function(){
    _state.node=el.getAttribute('data-id');_state.page=1;renderTable(_state.m);});});
  $$('#treePane .tw[data-toggle]').forEach(function(el){
    var id=el.getAttribute('data-toggle');if(!id)return;
    el.addEventListener('click',function(ev){ev.stopPropagation();
      if(_state.expanded[id])delete _state.expanded[id];else _state.expanded[id]=1;
      renderTree(_state.m);});});
}

/* 树状展示：按 idpath/idlevel 计算层级深度 */
function treeDepth(r){
  if(r.idlevel!=null&&r.idlevel!==''){var n=+r.idlevel;if(!isNaN(n))return Math.max(0,n-1);}
  if(r.idpath!=null&&r.idpath!==''){return Math.max(0,String(r.idpath).split('.').length-1);}
  return 0;
}
function renderRows(m,pageRows){
  var hid=_state.hiddenCols||[];
  var cols=m.cols.filter(function(c){return hid.indexOf(c.f)<0;});
  if(!cols.length)cols=m.cols.slice(0,1);
  /* 多选外键列：表格中优先显示联查名称（如 aaa 列显示 aaa_code 的聚合值） */
  var fkDisp={};
  (m.fields||[]).forEach(function(f){if(f.tp&&f.multi&&f.disp)fkDisp[f.f]=f.f+'_'+f.disp.split(',')[0];});
  function cellVal(r,c){
    var v=r[c.f];
    if(fkDisp[c.f]&&r[fkDisp[c.f]]!=null&&r[fkDisp[c.f]]!=='')v=r[fkDisp[c.f]];
    return v;
  }
  var hasAct=(m.ops.edit&&hasPerm(m.ops.edit))||(m.ops.del&&hasPerm(m.ops.del))||!!m.dt;
  var head='<tr>'+cols.map(function(c){return '<th>'+esc(c.t||cn(c.f))+'</th>';}).join('')+(hasAct?'<th style="text-align:right">操作</th>':'')+'</tr>';
  var body;
  if(!pageRows.length){
    body='<tr><td colspan="'+(cols.length+(hasAct?1:0))+'"><div class="empty">'+icon('list',34)+'<div>暂无数据</div></div></td></tr>';
  }else if(_state.view==='card'){
    /* 卡片视图（对应 bootstrap-table 的视图切换） */
    var cards=pageRows.map(function(r){
      var items=cols.map(function(c){
        var v=cellVal(r,c);var txt=(v===null||v===undefined)?'':(c.b?(isOn(v)?'启用':'停用'):String(v));
        return '<div class="card-item"><span>'+esc(cn(c.f))+'</span><b>'+esc(txt)+'</b></div>';
      }).join('');
      return '<div class="card-row" data-pk="'+esc(String(r[m.pk]!=null?r[m.pk]:''))+'"><div class="card-row__title">'+esc(nameOf(m,r))+'</div>'+items+'</div>';
    }).join('');
    $('#tbl').innerHTML='<div class="cards">'+(cards||'<div class="empty">暂无数据</div>')+'</div>';
    $$('#tbl .card-row').forEach(function(cd){
      cd.addEventListener('click',function(){
        $$('#tbl .card-row').forEach(function(x){x.classList.remove('selected');});
        cd.classList.add('selected');
        _state.sel=findRow(m,cd.getAttribute('data-pk'));
      });
    });
    return;
  }else{
    /* 树状展示（权限管理/部门管理等带 idpath 的模块）：
       后端已按 idpath 排序（父先子后），这里按 idlevel 缩进，
       有下级的行显示 ▼/▶，可折叠/展开其子树。 */
    var isTree=!!m.tree&&!$('#treePane')&&pageRows.some(function(r){return r.idpath!=null&&r.idpath!=='';});
    var collapsed=_state.collapsed||{};
    var visRows=pageRows;
    if(isTree){
      visRows=pageRows.filter(function(r){
        var p=r.idpath;if(p==null||p==='')return true;
        var parts=String(p).split('.');
        for(var i=1;i<parts.length;i++){
          if(collapsed[parts.slice(0,i).join('.')])return false;
        }
        return true;
      });
    }
    body=visRows.map(function(r){
      var tds=cols.map(function(c,ci){
        if(c.b)return '<td>'+boolBadge(r[c.f])+'</td>';
        var v=cellVal(r,c);var txt=(v===null||v===undefined)?'':esc(String(v));
        var td='<td'+(c.w?' class="nowrap-el"':'')+'>';
        if(isTree&&ci===0){
          var d=treeDepth(r);
          var tp=String(r.idpath==null?'':r.idpath);
          var hasKids=tp!==''&&pageRows.some(function(x){var xp=String(x.idpath||'');return xp!==''&&xp.indexOf(tp+'.')===0;});
          if(hasKids){
            td+='<span class="tree-toggle" data-tp="'+esc(tp)+'" title="展开/折叠">'+(collapsed[tp]?'▶':'▼')+'</span>';
          }else{
            td+='<span class="tree-leaf"></span>';
          }
          td+='<span class="tree-label">'+txt+'</span>';
          return td+'</td>';
        }
        return td+txt+(m.tree&&isCurNode&&ci===0?'<span class="cur-badge">当前</span>':'')+'</td>';
      }).join('');
      var pkv=esc(String(r[m.pk]!=null?r[m.pk]:''));
      var isCurNode=(m.tree&&_state.node!=null&&String(r.idpath||'')===String(_state.node));
      var selCls=(_state.sel&&String(_state.sel[m.pk])===String(r[m.pk]))?' class="selected"':(isCurNode?' class="cur-node"':'');
      var act='';
      if(m.ops.edit&&hasPerm(m.ops.edit))act+='<button class="btn btn--sm btn--ghost btn--icon" data-act="edit" data-pk="'+pkv+'" title="编辑">'+icon('edit',16)+'</button>';
      if(m.dt)act+='<button class="btn btn--sm btn--ghost btn--icon" data-act="detail" data-pk="'+pkv+'" title="权限">'+icon('eye',16)+'</button>';
      if(m.ops.del&&hasPerm(m.ops.del))act+='<button class="btn btn--sm btn--ghost btn--icon" data-act="del" data-pk="'+pkv+'" title="删除">'+icon('trash',16)+'</button>';
      return '<tr data-pk="'+pkv+'"'+selCls+'>'+tds+(hasAct?'<td style="text-align:right;white-space:nowrap">'+act+'</td>':'')+'</tr>';
    }).join('');
  }
  var tbl=$('#tbl');
  tbl.innerHTML='<table class="data"><thead>'+head+'</thead><tbody>'+body+'</tbody></table>';
  $$('#tbl tbody tr[data-pk]').forEach(function(tr){
    tr.addEventListener('click',function(){
      $$('#tbl tbody tr').forEach(function(x){x.classList.remove('selected');});
      tr.classList.add('selected');
      _state.sel=findRow(m,tr.getAttribute('data-pk'));
      /* 点击表格行：左树节点的选定态同步消失（当前记录改由表格行选定） */
      _state.node=null;
      $$('#treePane .tnode.on').forEach(function(x){x.classList.remove('on');});
    });
  });
  $$('#tbl .tree-toggle').forEach(function(sp){
    sp.addEventListener('click',function(ev){
      ev.stopPropagation();
      var tp=sp.getAttribute('data-tp');
      if(_state.collapsed[tp])delete _state.collapsed[tp];else _state.collapsed[tp]=1;
      renderTable(m);
    });
  });
  $$('#tbl [data-act]').forEach(function(b){
    b.addEventListener('click',function(ev){
      ev.stopPropagation();
      var tr=b.closest('tr');
      var row=tr?findRow(m,tr.getAttribute('data-pk')):null;
      if(!row)return;
      var a=b.getAttribute('data-act');
      if(a==='edit')openForm(m,row);
      else if(a==='detail')openDetail(m,row);
      else if(a==='del')doDel(m,row,'del');
    });
  });
}

function renderPager(total,pages){
  var p=$('#pager');if(!p)return;
  var btns='<button data-pg="prev" '+(_state.page<=1?'disabled':'')+'>上一页</button>';
  var dots=false;
  for(var i=1;i<=pages;i++){
    if(pages>7&&i>2&&i<pages-1&&Math.abs(i-_state.page)>1){
      if(!dots){btns+='<button disabled>…</button>';dots=true;}
      continue;
    }
    dots=false;
    btns+='<button data-pg="'+i+'" class="'+(i===_state.page?'active':'')+'">'+i+'</button>';
  }
  btns+='<button data-pg="next" '+(_state.page>=pages?'disabled':'')+'>下一页</button>';
  p.innerHTML='<div class="pager__info">共 '+total+' 条</div>'+btns;
  $$('#pager button[data-pg]').forEach(function(b){
    b.addEventListener('click',function(){
      var v=b.getAttribute('data-pg');
      if(v==='prev')_state.page--;else if(v==='next')_state.page++;else _state.page=+v;
      if(_state.m&&_state.m.id==='serverlog'){loadList(_state.m);return;} /* 后端分页：翻页重取 */
      renderTable(_state.m);
    });
  });
}

/* ============ 表单（新增 / 修改） ============ */
function openForm(m,row){
  var fields=m.fields;
  if(!fields||!fields.length){toast('该模块不支持新增','warn');return;}
  var isAdd=!row;
  var body='<div class="form-grid">'+fields.map(function(f){
    var full=f.w?' full':'';
    var val=row?(row[f.f]):'';
    if(val===null||val===undefined)val='';
    if(f.sw){
      var chk=isAdd?' checked':(isOn(row[f.f])?' checked':'');
      return '<div class="field'+full+'"><label>'+esc(f.t||cn(f.f))+'</label><label class="switch"><input type="checkbox" data-f="'+f.f+'"'+chk+'></label></div>';
    }
    if(f.tp){
      var tv=row&&row[f.f]!=null?String(row[f.f]):'';
      var lab='';   /* 外键显示列：取该外键第一个有值的联查列（如 aaa_code/aaa_dictname） */
      if(row){
        var dnames=(f.disp?f.disp.split(','):[]);
        for(var di=0;di<dnames.length;di++){
          var cv=row[f.f+'_'+dnames[di]];
          if(cv!=null&&String(cv)!==''){lab=String(cv);break;}
        }
      }
      if(!lab)lab=tv;
      return '<div class="field'+full+'"><label>'+esc(f.t||cn(f.f))+(f.r?' <span class="req">*</span>':'')+'</label>'
        +'<div class="tpick"><input class="input" type="text" data-f="'+f.f+'" data-tp="'+f.tp+'" data-value="'+esc(tv)+'" readonly placeholder="点击右侧按钮选择…" value="'+esc(lab)+'">'
        +'<button type="button" class="btn" data-tpick="'+f.tp+'" data-fname="'+f.f+'">'+icon('sitemap',15)+'选择</button></div></div>';
    }
    if(f.s){
      return '<div class="field'+full+'"><label>'+esc(f.t||cn(f.f))+'</label><select class="select" data-f="'+f.f+'" data-src="'+f.s+'"><option value="">加载中…</option></select></div>';
    }
    var type=f.n?'number':'text';
    var ro=(isAdd===false&&f.f===m.pk)?' disabled':'';
    return '<div class="field'+full+'"><label>'+esc(f.t||cn(f.f))+(f.r?' <span class="req">*</span>':'')+'</label>'
      +'<input class="input" type="'+type+'" data-f="'+f.f+'" value="'+esc(val)+'"'+ro+(f.r?' required':'')+'></div>';
  }).join('')+'</div>';
  /* 带 dt 的模块（父子表）：新增/编辑表单内嵌子表明细编辑区，保存时整表 JSON 一起提交 */
  var subCfg=(m.dt?subtableCfg(m.dt):null);
  var subRows=[];
  if(subCfg&&subCfg.fields){
    if(row&&row[subCfg.jsonKey])subRows=parseSubRows(row[subCfg.jsonKey]);
    body+='<div class="detail-head" style="margin-top:10px">'+esc(subCfg.t||'明细')+'<span style="flex:1"></span>'
      +'<button type="button" class="btn btn--primary btn--sm" data-fsub-add>'+icon('plus',14)+'新增'+esc(subCfg.t||'明细')+'</button></div>'
      +'<div class="card" style="margin-bottom:16px"><div class="card__body" style="padding:0"><div data-fsub-table></div></div></div>';
  }
  var modal=Modal.open({
    title:(isAdd?'新增 ':'编辑 ')+m.t,icon:isAdd?'plus':'edit',wide:(fields.length>4)||!!(subCfg&&subCfg.fields),body:body,
    footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button><button class="btn btn--primary" data-ok>'+icon('check',16)+'保存</button>'
  });
  fields.forEach(function(f){
    if(!f.s)return;
    var sel=modal.querySelector('[data-f="'+f.f+'"]');
    loadSource(f.s).then(function(opts){
      sel.innerHTML='<option value="">-- 请选择 --</option>'+opts;
      if(row&&row[f.f]!=null)sel.value=String(row[f.f]);
    }).catch(function(){sel.innerHTML='<option value="">-- 加载失败 --</option>';});
  });
  $$('[data-tpick]',modal).forEach(function(b){
    b.addEventListener('click',function(){
      var inp=b.parentElement.querySelector('input[data-tp]');
      if(!inp)return;
      var tp=b.getAttribute('data-tpick');
      var fld=fields.filter(function(x){return x.f===b.getAttribute('data-fname');})[0]||{};
      var done=function(id,label,all){
        if(fld.multi&&all){ /* 多选：以弹窗最终选中集合整体替换（支持增/减/清空） */
          inp.dataset.value=all.map(function(x){return x.id;}).join(',');
          inp._labs=all.map(function(x){return x.label;});
          inp.value=inp._labs.join(',');
          return;
        }
        if(fld.multi){ /* 兜底：逐行累积（显示与值统一用逗号分隔） */
          var ids=(inp.dataset.value||'').split(',').filter(Boolean);
          var labs=(inp._labs||(inp.value?inp.value.split(','):[])).slice();
          var i=ids.indexOf(String(id));
          if(i<0){ ids.push(String(id)); labs.push(label); }
          else labs[i]=label;
          while(labs.length<ids.length)labs.push(ids[labs.length]);
          inp.dataset.value=ids.join(','); inp._labs=labs; inp.value=labs.join(',');
        }else{ inp.dataset.value=String(id); inp.value=label; }
      };
      if(fld.lp){ /* 非树形主表：列表选择器（多选时弹窗内勾选、预带已选值） */
        var preIds=(fld.multi&&(inp.dataset.value||''))?inp.dataset.value.split(',').filter(Boolean):[];
        openListPicker(tp,done,function(){ /* 无权限降级：解除只读，允许手动输入 */
          inp.readOnly=false; inp.placeholder='无查询权限，请手动输入'; },!!fld.multi,preIds,fld.disp); return; }
      openTreePicker(tp,row,function(sel){
        var mm=byId(tp);
        done(sel[mm.pk],nameOf(mm,sel));
      });
    });
  });
  if(subCfg&&subCfg.fields){
    var fsubHost=modal.querySelector('[data-fsub-table]');
    function renderFSub(){
      if(!subRows.length){fsubHost.innerHTML='<div class="empty">暂无明细</div>';return;}
      var cols=subCfg.cols||SUBCOLS[m.dt]||[];
      var th=cols.map(function(c){return '<th>'+esc(c.t||cn(c.f))+'</th>';}).join('')+'<th style="width:110px">操作</th>';
      var tb=subRows.map(function(r,i){
        var tds=cols.map(function(c){
          var v=r[c.f];
          if(c.f==='permissiontype')v=(String(v)==='1'?'员工直接权限/功能权限':'角色权限/继承角色');
          if(c.f==='sysactionname'&&(v==null||v===''))v='#'+(r.sysactionid==null?'':r.sysactionid);
          if(c.f==='ifpermission')v=(isOn(v)?'允许':'禁止');
          return '<td>'+esc(v==null?'':String(v))+'</td>';
        }).join('');
        return '<tr>'+tds+'<td class="rowacts">'
          +'<button class="btn btn--sm btn--ghost btn--icon" data-fsub-edit="'+i+'" title="修改">'+icon('edit',14)+'</button>'
          +'<button class="btn btn--sm btn--ghost btn--icon" data-fsub-del="'+i+'" title="删除">'+icon('trash',14)+'</button>'
          +'</td></tr>';
      }).join('');
      fsubHost.innerHTML='<table class="data"><thead><tr>'+th+'</tr></thead><tbody>'+tb+'</tbody></table>';
      $$('[data-fsub-edit]',fsubHost).forEach(function(b){
        b.addEventListener('click',function(){
          var i=+b.getAttribute('data-fsub-edit');
          openSubRowEditor(subCfg,subRows[i],function(rec){subRows[i]=rec;renderFSub();});
        });
      });
      $$('[data-fsub-del]',fsubHost).forEach(function(b){
        b.addEventListener('click',async function(){
          var i=+b.getAttribute('data-fsub-del');
          if(!(await confirmBox('确定删除该明细行吗？','删除')))return;
          subRows.splice(i,1);renderFSub();
        });
      });
    }
    modal._subRows=subRows;
    renderFSub();
    var fab=modal.querySelector('[data-fsub-add]');
    if(fab)fab.addEventListener('click',function(){
      openSubRowEditor(subCfg,null,function(rec){subRows.push(rec);renderFSub();});
    });
  }
  modal.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  modal.querySelector('[data-ok]').addEventListener('click',function(){submitForm(m,row,modal);});
}

async function loadSource(srcKey){
  var s=SOURCES[srcKey];
  var j=await api(s.base+'_query',{token:TOKEN});
  var rows=(j.info&&j.info.rows)||[];
  return rows.map(function(r){return '<option value="'+esc(String(r[s.sv]))+'">'+esc(String(r[s.sl]))+'</option>';}).join('');
}

/* 按数据库类型校验输入值：不符返回错误文案，符合/未知类型返回空串。
   ty 为生成模块时写入 fields 的数据库类型（如 integer、varchar(32)、integer[]、timestamp）。 */
function fldTypeErr(ty,v){
  if(!ty||v==null||v==='')return '';
  var t=String(ty).toLowerCase().trim();
  if(t.slice(-2)==='[]'){          /* 数组列：逗号分隔，逐项按元素类型校验 */
    var et=t.slice(0,-2),parts=String(v).split(',');
    for(var i=0;i<parts.length;i++){
      var e=parts[i].trim();if(e==='')continue;
      var m=fldTypeErr(et,e);if(m)return '第'+(i+1)+'项'+m;
    }
    return '';
  }
  if(/^(smallint|integer|bigint|int2|int4|int8|int)$/.test(t))
    return /^-?\d+$/.test(String(v).trim())?'':'应为整数';
  if(/^(numeric|decimal|real|double precision|double|float4|float8|money)/.test(t))
    return /^-?\d+(\.\d+)?$/.test(String(v).trim())?'':'应为数字';
  if(/^(boolean|bool)$/.test(t))
    return /^(true|false|t|f|0|1|yes|no)$/i.test(String(v).trim())?'':'应为 true/false';
  if(t==='date')
    return /^\d{4}-\d{2}-\d{2}$/.test(String(v).trim())?'':'应为日期（YYYY-MM-DD）';
  if(t.indexOf('timestamp')===0||t.indexOf('timestamptz')===0)
    return /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?/.test(String(v).trim())?'':'应为日期时间（YYYY-MM-DD HH:MI:SS）';
  if(t.indexOf('time')===0)
    return /^\d{2}:\d{2}(:\d{2})?$/.test(String(v).trim())?'':'应为时间（HH:MI:SS）';
  var mm=t.match(/^(?:var)?char\s*\(\s*(\d+)\s*\)/);
  if(mm)return String(v).length<=+mm[1]?'':'长度不能超过 '+mm[1];
  return '';
}

async function submitForm(m,row,modal){
  var data={token:TOKEN};var ok=true;
  m.fields.forEach(function(f){
    var el=modal.querySelector('[data-f="'+f.f+'"]');if(!el)return;
    if(el.disabled)return;
    if(f.sw){data[f.f]=el.checked?'1':'0';return;}
    var v=(el.dataset&&el.dataset.value!=null&&el.dataset.value!=='')?el.dataset.value:el.value;
    if(f.r&&v===''){toast('请填写 '+cn(f.f),'err');ok=false;return;}
    if(v!==''){
      var er=fldTypeErr(f.ty,v);          /* 前端类型校验：输入与字段类型不符时阻止提交 */
      if(er){toast(cn(f.f)+' '+er,'err');ok=false;return;}
      data[f.f]=v;
    }
  });
  if(!ok)return;
  if(row)data[m.pk]=row[m.pk];
  /* 带 dt 的模块：子表明细整表 JSON 随主表一起提交（新增/修改均如此） */
  var subCfg=(m.dt?subtableCfg(m.dt):null);
  if(subCfg&&subCfg.fields&&modal._subRows){data[subCfg.jsonKey]=JSON.stringify(modal._subRows);}
  var path=m.base+(row?'_edit':'_add');
  try{
    await api(path,data);
    toast((row?'修改':'新增')+'成功','ok');
    Modal.close();loadList(m);
  }catch(e){toast(e.message||'操作失败','err');}
}

/* ============ 删除 / 恢复 ============ */
async function doDel(m,row,op){
  if(!row){toast('请先在表格中点击选择一行','warn');return;}
  op=op||'del';
  var verb=op==='undel'?'恢复':'删除';
  var pkv=(row[m.pk]!=null?row[m.pk]:'');
  if(!(await confirmBox('确定要'+verb+'记录【'+esc(cn(m.pk))+'：'+esc(String(pkv))+'】吗？',verb)))return;
  var data={token:TOKEN};data[m.pk]=row[m.pk];
  try{await api(m.base+'_'+op,data);toast(verb+'成功','ok');loadList(m);}
  catch(e){toast(e.message||verb+'失败','err');}
}

/* ============ 权限明细（dt 模块，支持子表增/改/删） ============ */
/* 子表配置：dt 值 → 子表 JSON 字段 / 父表编辑接口（整表 JSON 提交）/ 可编辑字段。
 * 不新增子表行级接口：增/改/删都在前端改内存数组，再把**整个子表**包装成 JSON
 * 传给父表编辑函数（operinfo_edit / roleinfo_edit），一次调用原子更新父子表；
 * 后端按子表主键做增删改（修改行必须携带主键），不整表先删后插，子表主键保持稳定。 */
var SUBTABLE={
  operpermission:{t:'员工权限',fk:'operatorid',pk:'permissionid',
    jsonKey:'operpermissionjson',                /* 查询返回的子表 JSON 字段 */
    editApi:'sysinfo/operinfo_edit',             /* 父表编辑接口（权限点 127，整表 JSON 原子更新） */
    perm:{add:126,edit:127,del:129},extra:['operatorname','accounts','tokentype','tokeninterval'],
    fields:[
      {f:'permissiontype',t:'sel',opts:[[1,'员工直接权限'],[2,'角色权限']],r:1},
      {f:'sysactionid',t:'perm',r:1},
      {f:'ifpermission',t:'sel',opts:[[1,'允许'],[0,'禁止']],r:1},
      {f:'permissionorder',t:'num'},
      {f:'params'}
    ]},
  rolepermission:{t:'角色权限',fk:'roleinfoid',pk:'permissionid',
    jsonKey:'rolepermissionjson',
    editApi:'sysinfo/roleinfo_edit',             /* 父表编辑接口（权限点 132） */
    perm:{add:132,edit:133,del:135},
    extra:['roleinfoname','description'],        /* 父表 _edit 还需要的父行字段 */
    fields:[
      {f:'permissiontype',t:'sel',opts:[[1,'功能权限'],[2,'继承角色']],r:1},
      {f:'sysactionid',t:'perm',r:1},
      {f:'ifpermission',t:'sel',opts:[[1,'允许'],[0,'禁止']],r:1},
      {f:'permissionorder',t:'num'},
      {f:'params'}
    ]}
};
/* 首次使用时把 gen_module 生成的 SUBTABLECFG 合入 SUBTABLE（SUBTABLE 声明在 init 之后，
 * 不能在 init 里合并，故在 openDetail 取配置时懒合并） */
function subtableCfg(key){
  try{(typeof SUBTABLECFG!=='undefined'?SUBTABLECFG:[]).forEach(function(c){
    if(c&&c.key&&SUBTABLE[c.key]===undefined)SUBTABLE[c.key]=c;
  });}catch(e){}
  return SUBTABLE[key];
}
/* 子表 JSON 字段解析：nginx 对 json 列返回的是嵌套数组/对象（不是字符串），
   其它来源也可能是 JSON 字符串——统一兼容解析为行数组，避免 JSON.parse(数组) 报错丢数据 */
function parseSubRows(v){
  if(v==null||v==='')return [];
  if(Array.isArray(v))return v;
  if(typeof v==='string'){
    try{var a=JSON.parse(v);return Array.isArray(a)?a:[];}catch(e){return [];}
  }
  return [];
}
/* 通用提交参数：token + 父主键(cfg.fk) + 父行附加字段(cfg.extra) + 子表整表 JSON */
function subBuildParams(cfg,row,rows){
  var d={token:TOKEN};
  d[cfg.fk]=row[cfg.fk];
  (cfg.extra||[]).forEach(function(k){d[k]=(row[k]==null?'':row[k]);});
  d[cfg.jsonKey]=JSON.stringify(rows);
  return d;
}
var SUBCOLS={
  operpermission:[{f:'permissionid'},{f:'permissiontype'},{f:'sysactionname'},{f:'ifpermission'},{f:'permissionorder'},{f:'params'}],
  rolepermission:[{f:'permissionid'},{f:'permissiontype'},{f:'sysactionname'},{f:'ifpermission'},{f:'permissionorder'},{f:'params'}]
};

function openDetail(m,row){
  if(!row){toast('请先选择一行','warn');return;}
  /* 子表明细模式：m.dt 为 SUBTABLE key（operpermission/rolepermission，
     或 gen_module 生成的父子表子表）。子表整表 JSON 一次性提交给父表 _edit。 */
  var cfg=subtableCfg(m.dt);
  var modal=Modal.open({title:m.t+' — 权限明细',icon:'key',wide:true,body:'<div class="spinner"></div>',footer:false});
  var body=modal.querySelector('.modal__body');
  var jsonKey=(cfg&&cfg.jsonKey)||null;
  if(!jsonKey){for(var k in row){if(/json$/i.test(k)&&typeof row[k]==='string'&&row[k]){jsonKey=k;break;}}}
  if(!cfg){ /* 未知子表类型：退回只读展示 */
    body.innerHTML=(jsonKey?'<div class="detail-head">已分配权限（'+esc(jsonKey)+'）</div><div class="card__body" style="margin-bottom:16px">'+permList(row[jsonKey])+'</div>':'')+recordInfo(row);
    return;
  }
  var fkVal=row[cfg.fk];
  var canAdd=cfg.perm.add&&hasPerm(cfg.perm.add);
  var canEdit=cfg.perm.edit&&hasPerm(cfg.perm.edit);
  var canDel=cfg.perm.del&&hasPerm(cfg.perm.del);
  body.innerHTML=
    '<div class="detail-head">'+esc(cfg.t)+'（'+esc(cn(cfg.fk))+'：'+esc(String(fkVal==null?'':fkVal))+'）'
    +'<span style="flex:1"></span>'
    +(canAdd?'<button class="btn btn--primary btn--sm" data-sub-add>'+icon('plus',14)+'新增权限</button>':'')
    +'</div>'
    +'<div class="card" style="margin-bottom:16px"><div class="card__body" style="padding:0"><div data-sub-table><div class="spinner"></div></div></div></div>'
    +'<div class="detail-head">记录信息</div>'+recordInfo(row);
  var tableHost=body.querySelector('[data-sub-table]');
  var rows=[];
  var ctx={m:m,modal:modal,cfg:cfg,jsonKey:jsonKey,row:row,fkVal:fkVal,
           tableHost:tableHost,canAdd:canAdd,canEdit:canEdit,canDel:canDel,
           getRows:function(){return rows;},setRows:function(v){rows=v;}};

  function renderSub(){
    if(!rows.length){tableHost.innerHTML='<div class="empty">未分配权限</div>';return;}
    var cols=cfg.cols||SUBCOLS[m.dt]||[];
    var th=cols.map(function(c){return '<th>'+esc(c.t||cn(c.f))+'</th>';}).join('')+'<th style="width:110px">操作</th>';
    var tb=rows.map(function(r,i){
      var tds=cols.map(function(c){
        var v=r[c.f];
        if(c.f==='permissiontype')v=(String(v)==='1'?'员工直接权限/功能权限':'角色权限/继承角色');
        if(c.f==='sysactionname'&&(v==null||v===''))v='#'+(r.sysactionid==null?'':r.sysactionid);
        if(c.f==='ifpermission')v=(isOn(v)?'允许':'禁止');
        return '<td>'+esc(v==null?'':String(v))+'</td>';
      }).join('');
      return '<tr>'+tds+'<td class="rowacts">'
        +(canEdit?'<button class="btn btn--sm btn--ghost btn--icon" data-sub-edit="'+i+'" title="修改">'+icon('edit',14)+'</button>':'')
        +(canDel?'<button class="btn btn--sm btn--ghost btn--icon" data-sub-del="'+i+'" title="删除">'+icon('trash',14)+'</button>':'')
        +'</td></tr>';
    }).join('');
    tableHost.innerHTML='<table class="data"><thead><tr>'+th+'</tr></thead><tbody>'+tb+'</tbody></table>';
    $$('[data-sub-edit]',tableHost).forEach(function(b){
      b.addEventListener('click',function(){openSubForm(ctx,rows[+b.getAttribute('data-sub-edit')]);});});
    $$('[data-sub-del]',tableHost).forEach(function(b){
      b.addEventListener('click',function(){delSubRow(ctx,rows[+b.getAttribute('data-sub-del')]);});});
  }
  ctx.render=renderSub;

  async function loadSub(){
    try{
      var arr=null;
      if(jsonKey)arr=parseSubRows(row[jsonKey]);
      if(!Array.isArray(arr))arr=[];
      rows=arr;
    }catch(e){rows=[];}
    renderSub();
  }
  ctx.load=loadSub;

  if(canAdd)body.querySelector('[data-sub-add]').addEventListener('click',function(){openSubForm(ctx,null);});
  loadSub();
}
/* ---- 子表行编辑弹窗（通用）：只负责收集/规范化一行字段，提交方式由调用方决定 ---- */
function openSubRowEditor(cfg,r,onDone){
  var isAdd=!r;
  var fhtml=cfg.fields.map(function(f){
    var v=r?r[f.f]:'';
    if(v===null||v===undefined||v==='')v=(f.t==='sel'?f.opts[0][0]:'');
    if(f.t==='sel'){
      return '<div class="field"><label>'+esc(cn(f.f))+(f.r?' <span class="req">*</span>':'')+'</label>'
        +'<select class="select" data-f="'+f.f+'">'+f.opts.map(function(o){
            return '<option value="'+o[0]+'"'+(String(o[0])===String(v)?' selected':'')+'>'+esc(o[1])+'</option>';
          }).join('')+'</select></div>';
    }
    if(f.t==='perm'){
      var label=r&&r.sysactionname?r.sysactionname:(r&&r.sysactionid!=null?('#'+r.sysactionid):'');
      var dv=r&&r.sysactionid!=null?(' data-value="'+esc(String(r.sysactionid))+'"'):'';
      return '<div class="field"><label>'+esc(cn(f.f))+' <span class="req">*</span></label>'
        +'<div class="tpick"><input class="input" type="text" data-f="'+f.f+'" data-perm readonly'+dv
        +' placeholder="点击右侧按钮选择权限…" value="'+esc(label)+'">'
        +'<button type="button" class="btn" data-permpick>'+icon('sitemap',15)+'选择</button></div></div>';
    }
    return '<div class="field"><label>'+esc(cn(f.f))+'</label>'
      +'<input class="input" type="'+(f.t==='num'?'number':'text')+'" data-f="'+f.f+'" value="'+esc(v)+'"></div>';
  }).join('');
  var fm=Modal.open({title:(isAdd?'新增':'修改')+(cfg.t||'明细')+'权限',icon:isAdd?'plus':'edit',
    body:'<div class="form-grid">'+fhtml+'</div>',
    footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button>'
      +'<button class="btn btn--primary" data-ok>'+icon('check',16)+'保存</button>'});
  $$('[data-permpick]',fm).forEach(function(b){
    b.addEventListener('click',function(){
      var pt=+fm.querySelector('[data-f="permissiontype"]').value;
      pickPermission(pt,function(id,label){
        var inp=fm.querySelector('[data-f="sysactionid"]');
        inp.dataset.value=id;inp.value=label;
      });
    });
  });
  fm.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  fm.querySelector('[data-ok]').addEventListener('click',function(){
    /* 只在前端收集该行字段，不直接调子表接口 */
    var rec={};var ok=true;
    cfg.fields.forEach(function(f){
      var el=fm.querySelector('[data-f="'+f.f+'"]');if(!el)return;
      var v=(el.dataset&&el.dataset.value!=null&&el.dataset.value!=='')?el.dataset.value:el.value;
      if(f.r&&(v===''||v==null)){toast('请填写 '+cn(f.f),'err');ok=false;return;}
      if(v!==''&&v!=null)rec[f.f]=v;
    });
    if(!ok)return;
    /* 规范化类型（按字段配置驱动），保证 json_to_recordset 能正确解析 */
    cfg.fields.forEach(function(f){
      if(!(f.f in rec))return;
      if(f.t==='num'||f.t==='sel')rec[f.f]=+rec[f.f];
      else rec[f.f]=String(rec[f.f]);
    });
    if(cfg.pk==='permissionid'){ /* 权限子表缺省值 */
      rec.ifpermission=(rec.ifpermission==null||isNaN(rec.ifpermission))?1:rec.ifpermission;
      rec.permissionorder=(rec.permissionorder==null||isNaN(rec.permissionorder))?0:rec.permissionorder;
      rec.params=(rec.params==null?'':String(rec.params));
    }
    /* 修改：携带子表主键（后端按主键更新，主键保持不变）；新增：不带主键由后端生成 */
    if(!isAdd)rec[cfg.pk]=r[cfg.pk];
    Modal.close();
    onDone(rec);
  });
}
/* ---- 子表行编辑：ctx=openDetail 上下文，r=null 表示新增；整表 JSON 提交父表编辑接口 ---- */
function openSubForm(ctx,r){
  var cfg=ctx.cfg,isAdd=!r;
  openSubRowEditor(cfg,r,function(rec){
    var arr=ctx.getRows().slice();
    if(isAdd){arr.push(rec);}
    else{
      var idx=-1;arr.forEach(function(x,i){if(String(x[cfg.pk])===String(r[cfg.pk]))idx=i;});
      if(idx<0){toast('数据已变化，请刷新后重试','err');return;}
      arr[idx]=rec;
    }
    saveSubtable(ctx,arr,(isAdd?'新增':'修改')+'成功');
  });
}

/* ---- 子表行删除：从内存数组移除该行，再整表 JSON 一次性提交 ---- */
async function delSubRow(ctx,r){
  if(!(await confirmBox('确定删除该权限行吗？','删除')))return;
  var arr=ctx.getRows().filter(function(x){return String(x[ctx.cfg.pk])!==String(r[ctx.cfg.pk]);});
  await saveSubtable(ctx,arr,'删除成功');
}

/* ---- 子表保存：把子表全部行（修改行带主键）包装成 JSON 传入父表编辑函数，后端按主键增删改 ---- */
async function saveSubtable(ctx,rows,okMsg){
  var cfg=ctx.cfg;
  try{
    await api(cfg.editApi,subBuildParams(cfg,ctx.row,rows));
    toast(okMsg||'保存成功','ok');
    ctx.setRows(rows);
    await refreshDetail(ctx);
  }catch(e){if(!e.handled)toast(e.message||'保存失败','err');}
}

/* ---- 子表变更后：刷新主表列表并原地更新明细（父行 JSON 已更新；不重开抽屉） ---- */
async function refreshDetail(ctx){
  await loadList(ctx.m);
  var fresh=ctx.row;
  try{
    var d={token:TOKEN,rows:20,page:1};d[ctx.m.pk]=ctx.row[ctx.m.pk];
    var j=await api(ctx.m.base+'_query',d);
    var rs=(j.info&&j.info.rows)||[];
    if(rs.length)fresh=rs[0];
  }catch(e){}
  ctx.row=fresh;
  if(ctx.setRows&&ctx.jsonKey)ctx.setRows(parseSubRows(fresh[ctx.jsonKey]));
  if(ctx.render)ctx.render();
}

/* ---- 权限选择器：type=1 选功能权限(sysaction 树)，type=2 选角色(roleinfo) ---- */
async function pickPermission(type,cb){
  if(type===2){
    var j=await api('sysinfo/roleinfo_query',{token:TOKEN,rows:200,page:1}).catch(function(){return null;});
    var rs=(j&&j.info&&j.info.rows)||[];
    var mm=Modal.open({title:'选择角色',icon:'tag',
      body:rs.length?('<table class="data"><thead><tr><th>编号</th><th>角色名</th><th>说明</th></tr></thead><tbody>'
        +rs.map(function(r){return '<tr data-rid="'+r.roleinfoid+'" data-rname="'+esc(r.roleinfoname)+'" style="cursor:pointer"><td>'+r.roleinfoid+'</td><td>'+esc(r.roleinfoname)+'</td><td>'+esc(r.description||'')+'</td></tr>';}).join('')
        +'</tbody></table>'):'<div class="empty">暂无角色</div>',
      footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button>'});
    mm.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
    $$('[data-rid]',mm).forEach(function(tr){
      tr.addEventListener('click',function(){
        cb(tr.getAttribute('data-rid'),tr.getAttribute('data-rname'));
        Modal.close();
      });
    });
    return;
  }
  /* 功能权限：复用通用树选择器选 sysaction 节点 */
  openTreePicker('sysaction',null,function(sel){
    var mm=byId('sysaction');
    cb(sel[mm.pk],nameOf(mm,sel));
  });
}

/* ---- 记录信息（只读键值表） ---- */
function recordInfo(row){
  return '<div class="card__body" style="padding-top:0"><table class="data"><tbody>'
    +Object.keys(row).filter(function(k){return !/json$/i.test(k);}).map(function(k){
      return '<tr><td style="width:34%;color:var(--muted)">'+esc(cn(k))+'</td><td>'+esc(row[k]==null?'':String(row[k]))+'</td></tr>';
    }).join('')+'</tbody></table></div>';
}

function permList(raw){
  var parsed=null;try{parsed=JSON.parse(raw);}catch(e){}
  if(parsed&&typeof parsed==='object'){
    var arr=Array.isArray(parsed)?parsed:Object.keys(parsed).map(function(k){return {name:k,val:parsed[k]};});
    if(!arr.length)return '<div class="empty">未分配权限</div>';
    return arr.map(function(it){
      if(it&&typeof it==='object'){var parts=[];Object.keys(it).forEach(function(k){if(it[k]!=null&&it[k]!=='')parts.push(cn(k)+': '+esc(String(it[k])));});
        return '<div style="padding:7px 0;border-bottom:1px solid var(--border)">'+(parts.join(' · ')||esc(JSON.stringify(it)))+'</div>';}
      return '<div style="padding:7px 0;border-bottom:1px solid var(--border)">'+esc(String(it))+'</div>';
    }).join('');
  }
  return '<pre style="white-space:pre-wrap;font-size:12.5px;color:var(--muted);margin:0">'+esc(String(raw))+'</pre>';
}

/* ============ 修改密码（对应 bootstrap 版 ChangePass，权限 141） ============ */
function openPassModal(){
  var m=Modal.open({title:'修改密码',icon:'lock',
    body:'<div class="form-grid">'
      +'<div class="field"><label>旧密码</label><input class="input" type="password" id="pass-old" autocomplete="new-password"></div>'
      +'<div class="field"><label>新密码</label><input class="input" type="password" id="pass-new" autocomplete="new-password"></div>'
      +'<div class="field"><label>确认新密码</label><input class="input" type="password" id="pass-new2" autocomplete="new-password"></div>'
      +'</div>',
    footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button><button class="btn btn--primary" data-ok>'+icon('check',16)+'保存</button>'});
  m.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  m.querySelector('[data-ok]').addEventListener('click',async function(){
    var o=$('#pass-old',m).value,n=$('#pass-new',m).value,n2=$('#pass-new2',m).value;
    if(!o||!n){toast('请填写旧密码与新密码','warn');return;}
    if(n!==n2){toast('两次输入的新密码不一致','warn');return;}
    try{
      await api('operator/changepassword',{token:TOKEN,oldpass:o,newpass:n});
      toast('密码修改成功','ok');Modal.close();
    }catch(e){ if(!e.handled)toast(e.message||'修改失败','err'); }
  });
  setTimeout(function(){var f=$('#pass-old',m);if(f)f.focus();},60);
}

/* ============ 显示列选择（对应 bootstrap-table showColumns） ============ */
function openColPicker(m){
  var hid=_state.hiddenCols||[];
  var items=m.cols.map(function(c,i){
    var dis=(m.tree&&i===0);
    var on=hid.indexOf(c.f)<0;
    return '<label class="colpick__item'+(dis?' dis':'')+'"><input type="checkbox" data-cf="'+c.f+'"'+(on?' checked':'')+(dis?' disabled':'')+'><span>'+esc(cn(c.f))+'</span></label>';
  }).join('');
  var md=Modal.open({title:'显示列',icon:'columns',body:'<div class="colpick">'+items+'</div>',
    footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button><button class="btn btn--primary" data-ok>'+icon('check',16)+'确定</button>'});
  md.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  md.querySelector('[data-ok]').addEventListener('click',function(){
    var sel=$$('.colpick input[data-cf]',md).filter(function(x){return x.checked;}).map(function(x){return x.getAttribute('data-cf');});
    _state.hiddenCols=m.cols.map(function(c){return c.f;}).filter(function(f){return sel.indexOf(f)<0;});
    Modal.close();renderTable(m);
  });
}

/* ============ 数据导出（对应 bootstrap-table showExport：Excel/Word/CSV） ============ */
function openExportMenu(m){
  var n=(_state.filtered||[]).length;
  var md=Modal.open({title:'导出数据',icon:'download',
    body:'<div class="export-opts">'
      +'<button class="btn" data-xls>'+icon('download',16)+'Excel (.xls)</button>'
      +'<button class="btn" data-doc>'+icon('download',16)+'Word (.doc)</button>'
      +'<button class="btn" data-csv>'+icon('download',16)+'CSV (.csv)</button>'
      +'</div><div class="export-note">导出范围为当前筛选结果（'+esc(m.t)+'，共 '+n+' 条），列与"显示列"设置一致。</div>'});
  var doExp=function(fmt){Modal.close();exportTable(m,fmt);};
  md.querySelector('[data-xls]').addEventListener('click',function(){doExp('xls');});
  md.querySelector('[data-doc]').addEventListener('click',function(){doExp('doc');});
  md.querySelector('[data-csv]').addEventListener('click',function(){doExp('csv');});
}
function exportTable(m,fmt){
  var hid=_state.hiddenCols||[];
  var cols=m.cols.filter(function(c){return hid.indexOf(c.f)<0;});
  if(!cols.length)cols=m.cols.slice(0,1);
  var rows=(_state.filtered&&_state.filtered.length)?_state.filtered:(_state.pageRows||[]);
  var name=String(m.t).replace(/[\\\/:*?"<>|]/g,'_');
  function txt(r,c){var v=r[c.f];if(c.b)v=isOn(v)?'启用':'停用';return (v===null||v===undefined)?'':String(v);}
  if(fmt==='csv'){
    var lines=[cols.map(function(c){return '"'+cn(c.f).replace(/"/g,'""')+'"';}).join(',')];
    rows.forEach(function(r){lines.push(cols.map(function(c){return '"'+txt(r,c).replace(/"/g,'""')+'"';}).join(','));});
    dl(new Blob(['\ufeff'+lines.join('\r\n')],{type:'text/csv;charset=utf-8'}),name+'.csv');
    toast('已导出 CSV（'+rows.length+' 条）','ok');return;
  }
  var thead='<tr>'+cols.map(function(c){return '<th>'+esc(cn(c.f))+'</th>';}).join('')+'</tr>';
  var tbody=rows.map(function(r){return '<tr>'+cols.map(function(c){return '<td>'+esc(txt(r,c))+'</td>';}).join('')+'</tr>';}).join('');
  var html='<html><head><meta charset="utf-8"></head><body><table border="1" cellspacing="0">'+thead+tbody+'</table></body></html>';
  if(fmt==='xls'){dl(new Blob(['\ufeff'+html],{type:'application/vnd.ms-excel;charset=utf-8'}),name+'.xls');}
  else{dl(new Blob(['\ufeff'+html],{type:'application/vnd.ms-word;charset=utf-8'}),name+'.doc');}
  toast('已导出 '+fmt.toUpperCase()+'（'+rows.length+' 条）','ok');
}
function dl(blob,filename){
  var a=document.createElement('a');var u=URL.createObjectURL(blob);
  a.href=u;a.download=filename;document.body.appendChild(a);a.click();
  setTimeout(function(){URL.revokeObjectURL(u);a.remove();},400);
}

/* ============ 通用树形选择弹窗（对应 bootstrap 版"选择上级/选择部门"zTree 弹窗） ============ */
function openTreePicker(srcModId,curRow,onOk){
  var mm=byId(srcModId);
  if(!mm){toast('未找到树数据源模块','err');return;}
  var title=srcModId==='sysorg'?'选择上级部门':(srcModId==='sysaction'?'选择上级权限':('选择'+mm.t));
  var md=Modal.open({title:title,icon:'sitemap',wide:true,
    body:'<div class="treepick"><div class="spinner"></div></div>',
    footer:'<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button><button class="btn btn--primary" data-ok disabled>'+icon('check',16)+'确定</button>'});
  md.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  api(mm.base+'_query',{token:TOKEN}).then(function(j){
    var rows=(j.info&&j.info.rows)||[];
    var children={};
    rows.forEach(function(r){
      var p=String(r.idpath||''),key='';
      if(p){var parts=p.split('.');if(parts.length>1)key=parts.slice(0,parts.length-1).join('.');}
      (children[key]||(children[key]=[])).push(r);
    });
    var selRow=null;
    function disabledRow(r){
      if(!curRow)return false;
      var cp=String(curRow[mm.pk]==null?'':curRow[mm.pk]);
      var rp=String(r.idpath||'');
      return rp===''||rp===String(curRow.idpath||'')||(curRow.idpath&&rp.indexOf(String(curRow.idpath)+'.')===0)||String(r[mm.pk])===cp;
    }
    function nodeLi(r,depth){
      var kids=children[String(r.idpath||'')]||[];
      var dis=disabledRow(r);
      var h='<li class="tpnode'+(dis?' dis':'')+'" data-ip="'+esc(String(r.idpath||''))+'" style="padding-left:'+(depth*18+8)+'px">'
        +icon(kids.length?'folder':'tag',14)+'<span>'+esc(nameOf(mm,r))+'</span>'+(dis?'<em>（自身/下级，禁选）</em>':'')+'</li>';
      kids.forEach(function(k){h+=nodeLi(k,depth+1);});
      return h;
    }
    var html='<ul class="tplist">';
    (children['']||[]).forEach(function(r){html+=nodeLi(r,0);});
    html+='</ul>';
    if(!rows.length){md.querySelector('.treepick').innerHTML='<div class="empty">暂无可选节点</div>';return;}
    md.querySelector('.treepick').innerHTML=html;
    var okBtn=md.querySelector('[data-ok]');
    $$('.tpnode',md).forEach(function(el){
      el.addEventListener('click',function(){
        if(el.classList.contains('dis')){toast('不能选择自身或其下级','warn');return;}
        $$('.tpnode',md).forEach(function(x){x.classList.remove('on');});
        el.classList.add('on');
        var ip=el.getAttribute('data-ip');
        selRow=null;rows.forEach(function(r){if(String(r.idpath||'')===ip)selRow=r;});
        okBtn.disabled=!selRow;
      });
    });
    okBtn.addEventListener('click',function(){
      if(!selRow){toast('请先选择一个节点','warn');return;}
      Modal.close();if(onOk)onOk(selRow);
    });
  }).catch(function(e){
    md.querySelector('.treepick').innerHTML='<div class="empty">'+icon('alert',30)+'<div>加载失败：'+esc(e.message||'')+'</div></div>';
  });
}

/* ---- 列表选择器：非树形主表的外键字段选择（点选一行回调，弹窗关闭） ----
 * disp：外键显示列（逗号分隔，来自 modules.js 字段配置，如 'code,dictname'）。
 * 弹窗列 = 编号(主键) + disp 各列，列名取主表模块的中文标题；未配置 disp 时
 * 回退为主表第一个含 name 的列（与列表页 nameOf 行为一致，兼容旧模块）。 */
function openListPicker(modId,onOk,onFail,multi,selIds,disp){
  var mm=byId(modId);
  if(!mm){toast('未找到数据源模块','err');if(onFail)onFail();return;}
  selIds=(selIds||[]).map(String);
  var dispCols=(disp==null?'':String(disp)).split(',').map(function(x){return x.trim();}).filter(Boolean);
  if(!dispCols.length){
    (mm.cols||[]).some(function(c){if(c.f&&c.f.toLowerCase().indexOf('name')>=0){dispCols=[c.f];return true;}return false;});
  }
  function colTitle(f){ /* 列名：优先表单字段标题，其次列表列标题，最后用字段名 */
    var src=(mm.fields||[]).concat(mm.cols||[]);
    for(var i=0;i<src.length;i++){if(src[i]&&src[i].f===f&&src[i].t)return src[i].t;}
    return f;
  }
  function cellVal(r,f){var v=r[f];return v==null?'':String(v);}
  function rowLabel(r){ /* 回显文本：取第一个有值的显示列，均空则回退主键 */
    for(var i=0;i<dispCols.length;i++){var s=cellVal(r,dispCols[i]).trim();if(s)return s;}
    return cellVal(r,mm.pk).trim();
  }
  var ncols=1+dispCols.length;
  var footer='<div class="toolbar__spacer"></div><button class="btn" data-c>取消</button>'
    +(multi?'<button class="btn btn--primary" data-ok>'+icon('check',16)+'确定</button>':'');
  var md=Modal.open({title:'选择'+mm.t+(multi?'（可多选）':''),icon:'list',wide:true,
    body:'<div style="max-height:60vh;overflow:auto"><table class="data"><thead><tr><th style="width:140px">编号</th>'
      +dispCols.map(function(f){return '<th>'+esc(colTitle(f))+'</th>';}).join('')
      +'</tr></thead><tbody><tr><td colspan="'+ncols+'"><div class="spinner"></div></td></tr></tbody></table></div>',
    footer:footer});
  md.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  api(mm.base+'_query',{token:TOKEN}).then(function(j){
    var rows=(j.info&&j.info.rows)||[];
    var tb=rows.map(function(r,i){
      var id=String(r[mm.pk]==null?'':r[mm.pk]);
      var pre=multi&&selIds.indexOf(id)>=0;
      return '<tr data-idx="'+i+'" data-id="'+esc(id)+'" data-sel="'+(pre?'1':'0')+'" style="cursor:pointer'+(pre?';background:#e8f0fe':'')+'"><td>'+esc(id)+'</td>'
        +dispCols.map(function(f){return '<td>'+esc(cellVal(r,f))+'</td>';}).join('')+'</tr>';
    }).join('')||'<tr><td colspan="'+ncols+'" class="empty">暂无数据</td></tr>';
    md.querySelector('tbody').innerHTML=tb;
    $$('tr[data-id]',md).forEach(function(tr){
      var r=rows[+tr.getAttribute('data-idx')];
      tr.addEventListener('click',function(){
        if(multi){ /* 多选：弹窗内切换选中态，点“确定”一次性带回 */
          var on=tr.getAttribute('data-sel')==='1';
          tr.setAttribute('data-sel',on?'0':'1');
          tr.style.background=on?'':'#e8f0fe';
          return;
        }
        var label=rowLabel(r)||tr.getAttribute('data-id');
        onOk(tr.getAttribute('data-id'),label);
        Modal.close();
      });
    });
    var okb=md.querySelector('[data-ok]');
    if(okb)okb.addEventListener('click',function(){
      var selTrs=$$('tr[data-sel="1"]',md);
      if(multi){ /* 多选：一次性带回全部选中行（整体替换语义，支持增/减/清空） */
        var selRows=selTrs.map(function(tr){
          var r=rows[+tr.getAttribute('data-idx')];
          return {id:tr.getAttribute('data-id'),label:rowLabel(r)||tr.getAttribute('data-id')};
        });
        Modal.close();if(onOk)onOk(null,null,selRows);return;
      }
      selTrs.forEach(function(tr){
        var r=rows[+tr.getAttribute('data-idx')];
        onOk(tr.getAttribute('data-id'),rowLabel(r)||tr.getAttribute('data-id'));
      });
      Modal.close();
    });
  }).catch(function(e){ /* 无查询权限/加载失败：关闭弹窗并允许手动输入 */
    Modal.close(); toast('无该主表查询权限，请手动输入','warn'); if(onFail)onFail();
  });
}
