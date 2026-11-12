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
  nav.innerHTML=html;
  $$('#sidebar-nav .nav-group__title').forEach(function(t){t.addEventListener('click',function(){t.parentElement.classList.toggle('closed');});});
  $$('#sidebar-nav .nav-item').forEach(function(a){a.addEventListener('click',function(){location.hash='#/'+a.getAttribute('data-mod');try{$('#app').classList.remove('nav-open');}catch(e){}});});
}

/* ============ 路由 ============ */
function route(){
  var id=(location.hash||'').replace(/^#\//,'');
  var m=byId(id);
  if(!m){id=defaultModule();location.replace('#/'+id);m=byId(id);}
  if(!m){$('#content').innerHTML='<div class="empty">无可用模块</div>';return;}
  if(!hasPerm(m.menu)){toast('无权限访问该模块','err');location.replace('#/'+defaultModule());return;}
  $$('#sidebar-nav .nav-item').forEach(function(a){a.classList.toggle('active',a.getAttribute('data-mod')===m.id);});
  if(m.id==='gethelp'){renderGetHelp(m);return;}
  renderModule(m);
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
    for(var i=1;i<=pages;i++){
      if(pages>7&&i>2&&i<pages-1&&Math.abs(i-_state.page)>1){if(btns.slice(-1)!=='…')btns+='<button disabled>…</button>';continue;}
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
    var j=await api(m.base+'_query',{token:TOKEN});
    _state.rows=(j.info&&j.info.rows)||[];
    _state.page=1;
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
      /* 右栏只显示当前选中节点的直接下级：_state.node 为选中节点 idpath，
         直接下级的 idpath = 该前缀 + '.' + 末段（其后不得再有 '.'） */
      var pre=String(_state.node)+'.';
      vis=rows.filter(function(r){
        var p=String(r.idpath||'');
        return p.indexOf(pre)===0&&p.indexOf('.',pre.length)<0;
      });
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
    var h='<div class="tnode'+(_state.node===id?' on':'')+'" style="padding-left:'+(6+depth*16)+'px" data-id="'+esc(id)+'">'
      +'<span class="tw'+(has?'':' leaf')+'" data-toggle="'+(has?id:'')+'" title="'+(has?(open?'折叠':'展开'):'')+'">'+(has?icon(open?'chev':'cher',13):'')+'</span>'
      +(has?icon(open?'folderOpen':'folder',14):'')
      +'<span class="tlabel">'+esc(nameOf(m,r))+'</span>'
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
  var hasAct=(m.ops.edit&&hasPerm(m.ops.edit))||(m.ops.del&&hasPerm(m.ops.del))||!!m.dt;
  var head='<tr>'+cols.map(function(c){return '<th>'+esc(cn(c.f))+'</th>';}).join('')+(hasAct?'<th style="text-align:right">操作</th>':'')+'</tr>';
  var body;
  if(!pageRows.length){
    body='<tr><td colspan="'+(cols.length+(hasAct?1:0))+'"><div class="empty">'+icon('list',34)+'<div>暂无数据</div></div></td></tr>';
  }else if(_state.view==='card'){
    /* 卡片视图（对应 bootstrap-table 的视图切换） */
    var cards=pageRows.map(function(r){
      var items=cols.map(function(c){
        var v=r[c.f];var txt=(v===null||v===undefined)?'':(c.b?(isOn(v)?'启用':'停用'):String(v));
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
        var v=r[c.f];var txt=(v===null||v===undefined)?'':esc(String(v));
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
        return td+txt+'</td>';
      }).join('');
      var pkv=esc(String(r[m.pk]!=null?r[m.pk]:''));
      var act='';
      if(m.ops.edit&&hasPerm(m.ops.edit))act+='<button class="btn btn--sm btn--ghost btn--icon" data-act="edit" data-pk="'+pkv+'" title="编辑">'+icon('edit',16)+'</button>';
      if(m.dt)act+='<button class="btn btn--sm btn--ghost btn--icon" data-act="detail" data-pk="'+pkv+'" title="权限">'+icon('eye',16)+'</button>';
      if(m.ops.del&&hasPerm(m.ops.del))act+='<button class="btn btn--sm btn--ghost btn--icon" data-act="del" data-pk="'+pkv+'" title="删除">'+icon('trash',16)+'</button>';
      return '<tr data-pk="'+pkv+'">'+tds+(hasAct?'<td style="text-align:right;white-space:nowrap">'+act+'</td>':'')+'</tr>';
    }).join('');
  }
  var tbl=$('#tbl');
  tbl.innerHTML='<table class="data"><thead>'+head+'</thead><tbody>'+body+'</tbody></table>';
  $$('#tbl tbody tr[data-pk]').forEach(function(tr){
    tr.addEventListener('click',function(){
      $$('#tbl tbody tr').forEach(function(x){x.classList.remove('selected');});
      tr.classList.add('selected');
      _state.sel=findRow(m,tr.getAttribute('data-pk'));
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
  for(var i=1;i<=pages;i++){
    if(pages>7&&i>2&&i<pages-1&&Math.abs(i-_state.page)>1){if(btns.slice(-1)!=='…')btns+='<button disabled>…</button>';continue;}
    btns+='<button data-pg="'+i+'" class="'+(i===_state.page?'active':'')+'">'+i+'</button>';
  }
  btns+='<button data-pg="next" '+(_state.page>=pages?'disabled':'')+'>下一页</button>';
  p.innerHTML='<div class="pager__info">共 '+total+' 条</div>'+btns;
  $$('#pager button[data-pg]').forEach(function(b){
    b.addEventListener('click',function(){
      var v=b.getAttribute('data-pg');
      if(v==='prev')_state.page--;else if(v==='next')_state.page++;else _state.page=+v;
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
      return '<div class="field'+full+'"><label>'+esc(cn(f.f))+'</label><label class="switch"><input type="checkbox" data-f="'+f.f+'"'+chk+'></label></div>';
    }
    if(f.tp){
      var tv=row&&row[f.f]!=null?String(row[f.f]):'';
      return '<div class="field'+full+'"><label>'+esc(cn(f.f))+(f.r?' <span class="req">*</span>':'')+'</label>'
        +'<div class="tpick"><input class="input" type="text" data-f="'+f.f+'" data-tp="'+f.tp+'" readonly placeholder="点击右侧按钮选择…" value="'+esc(tv)+'">'
        +'<button type="button" class="btn" data-tpick="'+f.tp+'">'+icon('sitemap',15)+'选择</button></div></div>';
    }
    if(f.s){
      return '<div class="field'+full+'"><label>'+esc(cn(f.f))+'</label><select class="select" data-f="'+f.f+'" data-src="'+f.s+'"><option value="">加载中…</option></select></div>';
    }
    var type=f.n?'number':'text';
    var ro=(isAdd===false&&f.f===m.pk)?' disabled':'';
    return '<div class="field'+full+'"><label>'+esc(cn(f.f))+(f.r?' <span class="req">*</span>':'')+'</label>'
      +'<input class="input" type="'+type+'" data-f="'+f.f+'" value="'+esc(val)+'"'+ro+(f.r?' required':'')+'></div>';
  }).join('')+'</div>';
  var modal=Modal.open({
    title:(isAdd?'新增 ':'编辑 ')+m.t,icon:isAdd?'plus':'edit',wide:fields.length>4,body:body,
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
      openTreePicker(b.getAttribute('data-tpick'),row,function(sel){
        var mm=byId(b.getAttribute('data-tpick'));
        inp.dataset.value=sel[mm.pk];
        inp.value=nameOf(mm,sel);
      });
    });
  });
  modal.querySelector('[data-c]').addEventListener('click',function(){Modal.close();});
  modal.querySelector('[data-ok]').addEventListener('click',function(){submitForm(m,row,modal);});
}

async function loadSource(srcKey){
  var s=SOURCES[srcKey];
  var j=await api(s.base+'_query',{token:TOKEN});
  var rows=(j.info&&j.info.rows)||[];
  return rows.map(function(r){return '<option value="'+esc(String(r[s.sv]))+'">'+esc(String(r[s.sl]))+'</option>';}).join('');
}

async function submitForm(m,row,modal){
  var data={token:TOKEN};var ok=true;
  m.fields.forEach(function(f){
    var el=modal.querySelector('[data-f="'+f.f+'"]');if(!el)return;
    if(el.disabled)return;
    if(f.sw){data[f.f]=el.checked?'1':'0';return;}
    var v=(el.dataset&&el.dataset.value!=null&&el.dataset.value!=='')?el.dataset.value:el.value;
    if(f.r&&v===''){toast('请填写 '+cn(f.f),'err');ok=false;return;}
    if(v!=='')data[f.f]=v;
  });
  if(!ok)return;
  if(row)data[m.pk]=row[m.pk];
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

/* ============ 权限明细（dt 模块） ============ */
function openDetail(m,row){
  if(!row){toast('请先选择一行','warn');return;}
  var modal=Modal.open({title:m.t+' — 权限明细',icon:'key',wide:true,body:'<div class="spinner"></div>',footer:false});
  var jsonKey=null;
  for(var k in row){if(/json$/i.test(k)&&typeof row[k]==='string'&&row[k]){jsonKey=k;break;}}
  var html='';
  if(jsonKey){
    html+='<div class="detail-head">已分配权限（'+esc(jsonKey)+'）</div><div class="card__body" style="margin-bottom:16px">'+permList(row[jsonKey])+'</div>';
  }
  html+='<div class="detail-head">记录信息</div><div class="card__body" style="padding-top:2px">';
  html+='<table class="data"><tbody>'+Object.keys(row).filter(function(k){return !/json$/i.test(k);}).map(function(k){
    return '<tr><td style="width:34%;color:var(--muted)">'+esc(cn(k))+'</td><td>'+esc(row[k]==null?'':String(row[k]))+'</td></tr>';
  }).join('')+'</tbody></table></div>';
  modal.querySelector('.modal__body').innerHTML=html;
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
