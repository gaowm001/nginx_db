'use strict';
/* NGXDB 本地管理台前端（零依赖）。后端 API 仅本机 127.0.0.1。 */
const API = (location.port === '8765' || location.protocol === 'file:') ? '' : 'http://127.0.0.1:8765';
const $ = id => document.getElementById(id);
/* 版本徽标自动显示实际加载的 admin.js 版本（取自 script src 的 v= 参数） */
(function () {
  const s = document.querySelector('script[src*="admin.js"]');
  const v = s ? (s.src.split('v=')[1] || '') : '';
  const el = document.getElementById('app-ver');
  if (el && v) el.textContent = 'JS v' + v;
})();
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&','<':'<','>':'>','"':'"',"'":'&#39;'}[c]));

async function api(path, body) {
  const opt = body ? {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)}
                    : {};
  const r = await fetch(API + path, opt);
  let d;
  try { d = await r.json(); } catch (e) { throw new Error('HTTP ' + r.status); }
  if (!r.ok || d.error) throw new Error(d.error || ('HTTP ' + r.status));
  return d;
}
function msg(id, text, ok) {
  const el = $(id);
  el.textContent = text || '';
  el.className = 'msg ' + (ok ? 'ok' : (text ? 'err' : ''));
}
const FIELD_TYPES = [
  {v: 'varchar', label: 'varchar 变长文本', len: true, def: '255'},
  {v: 'char', label: 'char 定长文本', len: true, def: '10'},
  {v: 'text', label: 'text 长文本'},
  {v: 'integer', label: 'integer 整数'},
  {v: 'bigint', label: 'bigint 长整数'},
  {v: 'smallint', label: 'smallint 小整数'},
  {v: 'numeric', label: 'numeric 精确小数', len: true, def: '12,2', ph: '精度,标度'},
  {v: 'real', label: 'real 单精度浮点'},
  {v: 'double precision', label: 'double 双精度浮点'},
  {v: 'money', label: 'money 金额'},
  {v: 'boolean', label: 'boolean 布尔'},
  {v: 'date', label: 'date 日期'},
  {v: 'time', label: 'time 时间'},
  {v: 'timestamp', label: 'timestamp 时间戳'},
  {v: 'timestamptz', label: 'timestamptz 带时区'},
  {v: 'interval', label: 'interval 时间间隔'},
  {v: 'uuid', label: 'uuid'},
  {v: 'json', label: 'json'},
  {v: 'jsonb', label: 'jsonb'},
  {v: 'bytea', label: 'bytea 二进制'},
  {v: 'inet', label: 'inet IP 地址'},
  {v: 'macaddr', label: 'macaddr MAC 地址'},
  {v: 'xml', label: 'xml'},
  {v: 'serial', label: 'serial 自增整数'},
  {v: 'bigserial', label: 'bigserial 自增长整数'}
];
function parseType(t) {
  t = (t || 'varchar(255)').trim();
  const m = t.match(/^([a-z ]+?)\s*(?:\(([^)]*)\))?$/i);
  return {base: (m ? m[1] : t).trim().toLowerCase(), args: (m && m[2] ? m[2] : '')};
}
function fieldTypeOf(row) {
  const ft = FIELD_TYPES.find(f => f.v === row.children[1].value) || FIELD_TYPES[0];
  if (!ft.len) return ft.v;
  const a = (row.children[2].value || '').trim();
  return a ? ft.v + '(' + a + ')' : ft.v;
}

/* ---------- 选项卡 ---------- */
document.querySelectorAll('.tab').forEach(t => t.onclick = () => {
  document.querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x === t));
  document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id === 'tab-' + t.dataset.tab));
  if (t.dataset.tab === 'pub') loadPublished();
  if (t.dataset.tab === 'sys') loadSys();
});
document.querySelectorAll('.stab').forEach(t => t.onclick = () => {
  document.querySelectorAll('.stab').forEach(x => x.classList.toggle('active', x === t));
  $('gen-sql').classList.toggle('active', t.dataset.stab === 'sql');
  $('gen-mjs').classList.toggle('active', t.dataset.stab === 'mjs');
  $('gen-prev').classList.toggle('active', t.dataset.stab === 'prev');
});

/* ---------- 弹窗 ---------- */
function openModal(id) { $(id).classList.add('show'); $('mask').classList.add('show'); }
function closeModal(id) { $(id).classList.remove('show'); $('mask').classList.remove('show'); }
document.addEventListener('click', e => {
  if (e.target.dataset && e.target.dataset.close !== undefined) {
    document.querySelectorAll('.modal.show').forEach(m => closeModal(m.id));
  }
});

/* ---------- 数据表管理 ---------- */
let TABLES = [];
let SCHEMAS = [];   /* 全部模式（/api/schemas）：外键等下拉使用，不受数据表页面当前模式筛选影响 */
async function loadSchemas() {
  const d = await api('/api/schemas');
  SCHEMAS = d.schemas || [];
  $('schema-sel').innerHTML = '<option value="">全部模式</option>' +
    SCHEMAS.map(s => '<option>' + esc(s) + '</option>').join('');
  $('nt-schema-sel').innerHTML = SCHEMAS.map(s => '<option>' + esc(s) + '</option>').join('');
  if (SCHEMAS.includes('sysinfo')) $('nt-schema-sel').value = 'sysinfo';
}
async function loadTables() {
  try {
    const schema = $('schema-sel').value;
    const d = await api('/api/tables' + (schema ? '?schema=' + schema : ''));
    TABLES = d.tables;
    renderTables();
    const sel = $('gen-table-sel');
    const cur = sel.value;
    sel.innerHTML = '<option value="">— 选择表 —</option>' +
      TABLES.map(t => '<option value="' + esc(t.schema) + '.' + esc(t.table) + '">' +
        esc(t.schema) + '.' + esc(t.table) + (t.comment ? '（' + esc(t.comment) + '）' : '') + '</option>').join('');
    sel.value = cur;
  } catch (e) {
    const box = $('tbl-list');
    if (box) box.querySelector('tbody').innerHTML =
      '<tr><td colspan="5" class="hint">表清单加载失败: ' + esc(e.message || '') + '</td></tr>';
  }
}
function renderTables() {
  try {
    const kw = $('tbl-search').value.trim().toLowerCase();
    const rows = TABLES.filter(t => !kw || t.table.includes(kw) || (t.comment || '').toLowerCase().includes(kw));
    $('tbl-list').querySelector('tbody').innerHTML = rows.map(t =>
      '<tr><td>' +
      '<button class="mini" onclick="showAlter(\'' + esc(t.schema) + '\',\'' + esc(t.table) + '\')">结构/修改</button>' +
      '<button class="mini" onclick="genPick(\'' + esc(t.schema) + '\',\'' + esc(t.table) + '\')">生成模块</button>' +
      '<button class="mini danger" onclick="dropTable(\'' + esc(t.schema) + '\',\'' + esc(t.table) + '\')">删除</button>' +
      '</td><td><b>' + esc(t.table) + '</b></td><td>' + esc(t.schema) + '</td><td>' + esc(t.comment || '') +
      '</td><td>' + esc(t.size || '') + '</td></tr>').join('') || '<tr><td colspan="5" class="hint">无表</td></tr>';
  } catch (e) {
    const box = $('tbl-list');
    if (box) box.querySelector('tbody').innerHTML =
      '<tr><td colspan="5" class="hint">渲染失败: ' + esc(e.message || '') + '</td></tr>';
  }
}
$('btn-refresh').onclick = loadTables;
$('schema-sel').onchange = loadTables;
$('tbl-search').oninput = renderTables;

/* 新建表 / 修改表（同一表单：修改时预填表信息） */
function addFieldRow(name, type, cn, box, opts) {
  opts = opts || {};
  const spec = parseType(type);
  const div = document.createElement('div');
  div.className = 'nt-field' + (opts.locked ? ' locked' : '');
  div.innerHTML = '<input placeholder="字段名" value="' + esc(name || '') + '"' + (opts.locked ? ' disabled' : '') + '>' +
    '<select' + (opts.locked ? ' disabled' : '') + '>' + FIELD_TYPES.map(ft => '<option value="' + ft.v + '"' + (ft.v === spec.base ? ' selected' : '') + '>' + ft.label + '</option>').join('') + '</select>' +
    '<input class="tlen" placeholder="长度" value="' + esc(spec.args) + '"' + (opts.locked ? ' disabled' : '') + '>' +
    '<input placeholder="中文含义" value="' + esc(cn || '') + '">' +
    '<label style="display:flex;align-items:center;gap:4px;color:var(--dim)"><input type="checkbox" class="req"' + (opts.req ? ' checked' : '') + (opts.locked ? ' disabled' : '') + '>必填</label>' +
    (opts.locked ? '' : '<button class="del" onclick="delFieldRow(this)" title="删除该字段">删除</button>');
  const sel = div.querySelector('select'), len = div.querySelector('.tlen');
  const sync = () => {
    const ft = FIELD_TYPES.find(f => f.v === sel.value) || FIELD_TYPES[0];
    len.style.display = ft.len ? '' : 'none';
    len.placeholder = ft.ph || '长度';
    if (ft.len && !len.value) len.value = ft.def;
  };
  sel.onchange = sync; sync();
  (box || $('nt-fields')).appendChild(div);
}
$('btn-add-field').onclick = () => addFieldRow();
$('btn-add-isused').onclick = () => addFieldRow('isused', 'smallint', '启用标志', $('nt-fields'));
$('btn-add-sysid').onclick = () => addFieldRow('systemid', 'integer', '归属系统', $('nt-fields'));
$('btn-add-cfield').onclick = () => addFieldRow('', '', '', $('nt-child-fields'));
function fillTreeStructFields() {
  if ($('nt-tree-fields').children.length) return;
  /* 树表默认结构列均为必填项（upid/idpath/idlevel/idcount 由生成函数自动维护，展示为只读+必填） */
  addFieldRow('upid', 'bigint', '上级编号（0=顶级）', $('nt-tree-fields'), {locked: true, req: true});
  addFieldRow('idpath', 'varchar(255)', '节点路径', $('nt-tree-fields'), {locked: true, req: true});
  addFieldRow('idlevel', 'integer', '层级', $('nt-tree-fields'), {locked: true, req: true});
  addFieldRow('idcount', 'integer', '子树节点数', $('nt-tree-fields'), {locked: true, req: true});
}
/* ===== 外键引用（public.fk_meta）：外键字段下拉随字段联动；主表按“模式→表”两级下拉选择 ===== */
function fkColOptions() {
  return [...$('nt-fields').querySelectorAll('.nt-field')]
    .filter(r => !r.classList.contains('locked'))
    .map(r => r.children[0].value.trim()).filter(Boolean);
}
function refreshFkColOptions() {
  if (!$('nt-fk-list')) return;
  const cols = fkColOptions();
  [...$('nt-fk-list').querySelectorAll('.nt-fk')].forEach(r => {
    const sel = r.querySelector('.fk-col'), cur = sel.value;
    sel.innerHTML = '<option value="">外键字段</option>' +
      cols.map(c => '<option value="' + esc(c) + '"' + (c === cur ? ' selected' : '') + '>' + esc(c) + '</option>').join('');
  });
}
function fkFillSchemas(sel, cur) {
  /* 模式下拉显示全部模式（SCHEMAS 来自 /api/schemas）；仅当接口不可用时退回已加载表的去重模式 */
  const schemas = (SCHEMAS && SCHEMAS.length) ? SCHEMAS : [...new Set(TABLES.map(t => t.schema))];
  sel.innerHTML = '<option value="">模式</option>' +
    schemas.map(s => '<option value="' + esc(s) + '"' + (s === cur ? ' selected' : '') + '>' + esc(s) + '</option>').join('');
}
let FK_REQ_SEQ = 0;
async function fkFillTables(sel, schema, curTable) {
  /* 主表下拉按模式实时拉取该模式全部表，不依赖数据表页面当前的筛选结果 */
  const myReq = ++FK_REQ_SEQ;
  sel.dataset.reqId = String(myReq);
  let ts = [];
  try {
    const d = await api('/api/tables' + (schema ? '?schema=' + encodeURIComponent(schema) : ''));
    ts = d.tables || [];
  } catch (e) { ts = TABLES.filter(t => !schema || t.schema === schema); }
  if (sel.dataset.reqId !== String(myReq)) return;   /* 已有更新的选择，丢弃过期响应 */
  sel.innerHTML = '<option value="">主表</option>' +
    ts.map(t => '<option value="' + esc(t.table) + '"' + (t.table === curTable ? ' selected' : '') + '>' +
      esc(t.table) + (t.comment ? '（' + esc(t.comment) + '）' : '') + '</option>').join('');
}
function addFkRow(box, data) {
  data = data || {};
  const div = document.createElement('div');
  div.className = 'nt-fk';
  div.innerHTML =
    '<div class="fk-line">' +
      '<select class="fk-col"></select>' +
      '<span class="arrow">→</span>' +
      '<select class="fk-ms"></select>' +
      '<select class="fk-mt"></select>' +
      '<input class="fk-mc" placeholder="主字段" disabled>' +
      '<input class="fk-db" placeholder="主库(默认本机)" value="local">' +
      '<span class="fk-type" style="color:var(--dim)">类型：-</span>' +
      '<button class="del">×</button>' +
    '</div>' +
    '<div class="fk-line">' +
      '<label class="lbl"><input type="checkbox" class="fk-multi">多选</label>' +
      '<select class="fk-disp" multiple></select>' +
      '<input class="fk-mod" placeholder="主表模块id(可空)">' +
    '</div>';
  const target = (typeof box === 'string') ? $(String(box).replace(/^#/, '')) : (box || $('nt-fk-list'));
  target.appendChild(div);
  const q = c => div.querySelector(c);
  let curBase = '';
  const reqType = () => (q('.fk-multi').checked ? curBase + '[]' : curBase);
  const updateFkType = () => { q('.fk-type').textContent = '类型：' + (curBase ? (q('.fk-multi').checked ? curBase + '[]' : curBase) : '-'); };
  const applyFkTypeToField = (fieldName, rtype) => {
    if (!fieldName || !rtype) return;
    const row = [...$('nt-fields').querySelectorAll('.nt-field')].filter(r => r.children[0].value.trim() === fieldName)[0];
    if (!row) return;
    const sel = row.querySelector('select');
    if (!sel) return;
    let opt = [...sel.options].filter(o => o.value === rtype)[0];
    if (!opt) { opt = document.createElement('option'); opt.value = rtype; opt.textContent = rtype + '（外键）'; sel.appendChild(opt); }
    sel.value = rtype;
    const len = row.querySelector('.tlen'); if (len) len.style.display = 'none';
  };
  const loadMaster = async () => {
    const ms = q('.fk-ms').value, mt = q('.fk-mt').value;
    q('.fk-mc').value = '';
    q('.fk-disp').innerHTML = '<option value="">先选主表</option>';
    if (!ms || !mt) return;
    try {
      const d = await api('/api/table?schema=' + encodeURIComponent(ms) + '&table=' + encodeURIComponent(mt));
      q('.fk-mc').value = d.pk || '';
      if (!q('.fk-mod').value) q('.fk-mod').value = mt; /* 主表模块 id 未填时自动带出（约定=表名） */
      const pkc = (d.columns || []).filter(c => c.name === d.pk)[0] || {};
      curBase = pkc.data_type === 'character varying' && pkc.len ? 'varchar(' + pkc.len + ')'
        : (pkc.data_type === 'numeric' && pkc.prec ? 'numeric(' + pkc.prec + ',' + pkc.scale + ')' : (pkc.data_type || 'bigint'));
      updateFkType();
      applyFkTypeToField(q('.fk-col').value, reqType());
      const defaults = (data.display_columns || '').split(',').map(x => x.trim()).filter(Boolean);
      q('.fk-disp').innerHTML = d.columns.map(c =>
        '<option value="' + esc(c.name) + '"' + (defaults.indexOf(c.name) >= 0 ? ' selected' : '') + '>' +
        esc(c.name) + (c.comment ? '（' + esc(c.comment) + '）' : '') + '</option>').join('');
    } catch (e) { toast(e.message || '加载主表信息失败', 'err'); }
  };
  fkFillSchemas(q('.fk-ms'), data.master_schema || $('nt-schema-sel').value);
  fkFillTables(q('.fk-mt'), q('.fk-ms').value, data.master_table);
  q('.fk-ms').onchange = () => { fkFillTables(q('.fk-mt'), q('.fk-ms').value, ''); loadMaster(); };
  q('.fk-mt').onchange = loadMaster;
  q('.fk-col').onchange = () => applyFkTypeToField(q('.fk-col').value, reqType());
  q('.fk-multi').onchange = () => { updateFkType(); applyFkTypeToField(q('.fk-col').value, reqType()); };
  if (data.fk_column) q('.fk-col').value = data.fk_column;
  if (data.master_column) q('.fk-mc').value = data.master_column;
  if (data.master_multi) q('.fk-multi').checked = true;
  if (data.master_module) q('.fk-mod').value = data.master_module;
  if (data.master_db) q('.fk-db').value = data.master_db;
  q('.del').onclick = () => div.remove();
  if (data.master_table) loadMaster();
  refreshFkColOptions();
}
function collectFks(box) {
  const root = (typeof box === 'string') ? $(String(box).replace(/^#/, '')) : (box || $('nt-fk-list'));
  return [...root.querySelectorAll('.nt-fk')].map(r => ({
    fk_column: r.querySelector('.fk-col').value.trim(),
    master_schema: r.querySelector('.fk-ms').value,
    master_table: r.querySelector('.fk-mt').value,
    master_column: r.querySelector('.fk-mc').value.trim(),
    req_type: ((r.querySelector('.fk-type') || {}).textContent || '').replace(/^类型：/, '').trim(),
    master_multi: r.querySelector('.fk-multi').checked ? 1 : 0,
    display_columns: [...r.querySelector('.fk-disp').selectedOptions].map(o => o.value).join(','),
    master_module: r.querySelector('.fk-mod').value.trim(),
    master_db: r.querySelector('.fk-db').value.trim() || 'local'
  })).filter(f => f.fk_column && f.master_table && f.master_schema);
}
function bindBtn(id, fn) { const el = $(id); if (el) el.onclick = fn; }
/* 删除字段行：修改表模式下该列保存时将从表中删除（含数据），需确认 */
function delFieldRow(btn) {
  const row = btn.parentNode;
  const name = ((row.children[0] || {}).value || '').trim();
  if (EDITMODE && name && !confirm('确定删除字段 ' + name + ' ？保存后该列将从表中删除（含数据）。')) return;
  row.remove();
  refreshFkColOptions();
}
bindBtn('btn-add-fk', () => addFkRow('#nt-fk-list'));
/* 字段增删/改名后，同步刷新外键行的“外键字段”下拉选项 */
new MutationObserver(() => refreshFkColOptions()).observe($('nt-fields'), {childList: true, subtree: true});
$('nt-fields').addEventListener('input', e => {
  if (e.target.tagName === 'INPUT' && e.target.parentElement && e.target.parentElement.children[0] === e.target) refreshFkColOptions();
});

/* 分库外键（postgres_fdw 外部数据访问） */
function openFdw() { msg('fdw-msg', ''); openModal('modal-fdw'); }
bindBtn('btn-fdw', openFdw);
bindBtn('btn-fdw-setup', async () => {
  try {
    const d = await api('/api/fkfdw/setup', {
      host: $('fdw-host').value.trim(), port: $('fdw-port').value.trim(),
      dbname: $('fdw-db').value.trim(), user: $('fdw-user').value.trim(),
      password: $('fdw-pass').value
    });
    msg('fdw-msg', '映射建立成功：server=' + d.server + '，fdwext 模式外部表=' + (d.tables.join('、') || '（无匹配的 fk_meta 配置，请先在外键里填写主库）'), true);
  } catch (e) { msg('fdw-msg', e.message); }
});
bindBtn('btn-fdw-rewrite', async () => {
  try {
    const d = await api('/api/fkfdw/rewrite', {});
    const parts = [];
    if (d.changed.length) parts.push('已改写：' + d.changed.join('；'));
    if (d.skipped.length) parts.push('跳过：' + d.skipped.join('；'));
    msg('fdw-msg', parts.join('　') || '没有需要改写的函数', true);
  } catch (e) { msg('fdw-msg', e.message); }
});
$('nt-kind').onchange = () => {
  const k = $('nt-kind').value;
  $('nt-child-box').style.display = k === 'parent_child' ? '' : 'none';
  $('nt-tree-box').style.display = k === 'tree' ? '' : 'none';
  if (k === 'tree') fillTreeStructFields();
};
$('btn-new-table').onclick = () => {
  EDITMODE = null;
  $('modal-title').textContent = '新建表';
  $('nt-fields').innerHTML = ''; $('nt-child-fields').innerHTML = ''; $('nt-tree-fields').innerHTML = ''; $('nt-fk-list').innerHTML = '';
  $('nt-kind').value = 'single'; $('nt-kind').disabled = false; $('nt-child-box').style.display = 'none'; $('nt-tree-box').style.display = 'none';
  if ($('schema-sel').value) $('nt-schema-sel').value = $('schema-sel').value; /* 默认继承数据表页面当前选中的模式 */
  $('nt-schema-sel').disabled = false; $('nt-table').disabled = false; $('nt-pk').disabled = false;
  $('nt-table').value = ''; $('nt-pk').value = ''; $('nt-chinese').value = '';
  $('btn-create-table').style.display = ''; $('btn-save-table').style.display = 'none';
  addFieldRow(); msg('nt-msg', ''); openModal('modal-new');
};
$('btn-create-table').onclick = async () => {
  const fields = [...$('nt-fields').querySelectorAll('.nt-field')].map(r => ({
    name: r.children[0].value.trim(), type: fieldTypeOf(r),
    cn: r.children[3].value.trim(), required: r.querySelector('.req').checked ? 1 : 0
  })).filter(f => f.name);
  const body = {schema: $('nt-schema-sel').value, table: $('nt-table').value.trim(),
    chinese: $('nt-chinese').value.trim(), pk: $('nt-pk').value.trim(), fields,
    kind: $('nt-kind').value, fks: collectFks('#nt-fk-list')};
  (body.fks || []).forEach(fk => {
    if (!fk.req_type) return;
    const f = body.fields.filter(x => x.name === fk.fk_column)[0];
    if (f) f.type = fk.req_type;
  });
  if (!body.schema) { msg('nt-msg', '请选择模式'); return; }
  if (body.kind === 'parent_child' && $('nt-child-table').value.trim()) {
    body.child_table = $('nt-child-table').value.trim();
    body.child_chinese = $('nt-child-cn').value.trim();
    body.child_pk = $('nt-child-pk').value.trim();
    body.child_fk = $('nt-child-fk').value.trim() || 'upid';
    body.child_fields = [...$('nt-child-fields').querySelectorAll('.nt-field')].map(r => ({
      name: r.children[0].value.trim(), type: fieldTypeOf(r),
      cn: r.children[3].value.trim(), required: r.querySelector('.req').checked ? 1 : 0
    })).filter(f => f.name);
  }
  try {
    await api('/api/table/create', body);
    msg('nt-msg', '建表成功', true);
    await loadTables();
    setTimeout(() => closeModal('modal-new'), 600);
  } catch (e) { msg('nt-msg', e.message); }
};

/* 新建模式 */
function openNewSchema() {
  $('ns-name').value = ''; $('ns-cn').value = ''; msg('ns-msg', '');
  openModal('modal-schema');
}
$('btn-new-schema').onclick = openNewSchema;
$('btn-new-schema-top').onclick = openNewSchema;
$('btn-create-schema').onclick = async () => {
  try {
    const d = await api('/api/schema/create', {schema: $('ns-name').value.trim(), chinese: $('ns-cn').value.trim()});
    msg('ns-msg', '模式 ' + d.schema + ' 创建成功', true);
    await loadSchemas();
    $('nt-schema-sel').value = d.schema;
    setTimeout(() => closeModal('modal-schema'), 700);
  } catch (e) { msg('ns-msg', e.message); }
};

/* 修改表：与新建表单一致，预填表信息（结构列只读） */
let EDITMODE = null;
async function showAlter(schema, table) {
  const d = await api('/api/table?schema=' + schema + '&table=' + table);
  EDITMODE = {schema, table};
  $('modal-title').textContent = '修改表：' + schema + '.' + table;
  const isTree = (d.columns || []).some(c => c.name === 'idpath');
  $('nt-kind').value = isTree ? 'tree' : 'single'; $('nt-kind').disabled = true;
  $('nt-tree-box').style.display = 'none'; /* 树表结构列已在下方字段列表以只读展示 */
  $('nt-schema-sel').value = schema; $('nt-schema-sel').disabled = true;
  $('nt-table').value = table; $('nt-table').disabled = true;
  $('nt-pk').value = d.pk || ''; $('nt-pk').disabled = true;
  $('nt-chinese').value = d.comment || table;
  $('nt-child-box').style.display = 'none';
  const structSet = new Set([d.pk, 'isused', 'systemid', 'upid', 'idpath', 'idlevel', 'idcount']);
  const typeOf = c => c.data_type === 'character varying' && c.len ? 'varchar(' + c.len + ')'
    : c.data_type === 'numeric' && c.prec ? 'numeric(' + c.prec + ',' + c.scale + ')' : c.data_type;
  /* 必填回显：数据库非空列（is_nullable=NO）与树表结构列均为必填项 */
  const treeStruct = new Set(['upid', 'idpath', 'idlevel', 'idcount']);
  const isReq = c => c.nullable === 'NO' || treeStruct.has(c.name);
  $('nt-fields').innerHTML = '';
  d.columns.filter(c => structSet.has(c.name)).forEach(c =>
    addFieldRow(c.name, typeOf(c), c.comment || '', $('nt-fields'),
      {locked: true, req: isReq(c)}));
  d.columns.filter(c => !structSet.has(c.name)).forEach(c =>
    addFieldRow(c.name, typeOf(c), c.comment || c.name, $('nt-fields'), {req: isReq(c)}));
  /* 外键配置读取放在字段列表填充之后：外键行的“外键字段”下拉选项来自字段列表 */
  $('nt-fk-list').innerHTML = ''; /* 先清空外键行容器：上次打开残留的行会与本次读取的配置叠加，导致外键越开越多 */
  try {
    const fkd = await api('/api/fk?schema=' + encodeURIComponent(schema) + '&table=' + encodeURIComponent(table));
    msg('nt-msg', '外键配置：' + ((fkd.fks || []).length) + ' 条', true);
    (fkd.fks || []).forEach(fk => {
      try { addFkRow('#nt-fk-list', fk); } catch (e) { msg('nt-msg', '外键行错误: ' + e.message); }
    });
  } catch (e) { msg('nt-msg', '外键读取失败: ' + e.message); }
  $('btn-create-table').style.display = 'none';
  $('btn-save-table').style.display = '';
  msg('nt-msg', '');
  openModal('modal-new');
}
$('btn-save-table').onclick = async () => {
  if (!EDITMODE) return;
  const fields = [...$('nt-fields').querySelectorAll('.nt-field')].map(r => ({
    name: r.children[0].value.trim(), type: fieldTypeOf(r),
    cn: r.children[3].value.trim()})).filter(f => f.name);
  collectFks('#nt-fk-list').forEach(fk => {
    if (!fk.req_type) return;
    const f = fields.filter(x => x.name === fk.fk_column)[0];
    if (f) f.type = fk.req_type;
  });
  try {
    const d = await api('/api/table/update_fields', {schema: EDITMODE.schema, table: EDITMODE.table, fields});
    await api('/api/fk/save', {schema: EDITMODE.schema, table: EDITMODE.table, fks: collectFks('#nt-fk-list')});
    msg('nt-msg', '已保存（' + d.changed + ' 项修改）', true);
    EDITMODE = null;
    await loadTables();
    setTimeout(() => closeModal('modal-new'), 700);
  } catch (e) { msg('nt-msg', e.message); }
};
async function dropTable(schema, table) {
  if (!confirm('确定删除表 ' + schema + '.' + table + ' ？\n将同时删除其生成的 6 个函数与权限节点（如存在）。')) return;
  try {
    await api('/api/table/drop', {schema, table, with_funcs: true});
    await loadTables();
  } catch (e) { alert('删除失败: ' + e.message); }
}

/* ---------- 模块生成 ---------- */
let GEN = null;
async function genPick(schema, table) {
  document.querySelector('.tab[data-tab=gen]').click();
  $('gen-table-sel').value = schema + '.' + table;
  await loadGenFields();
}
$('gen-table-sel').onchange = loadGenFields;
function renderFieldList(el, columns, pk) {
  const skip = new Set([pk, 'isused', 'systemid', 'upid', 'idpath', 'idlevel', 'idcount']);
  el.innerHTML = columns.filter(c => !skip.has(c.name)).map(c =>
    '<div class="field-item"><input type="checkbox" checked data-name="' + esc(c.name) + '">' +
    '<span class="fname">' + esc(c.name) + '</span><span class="ftype">' + esc(c.data_type) + (c.len ? '(' + c.len + ')' : '') +
    '</span><input type="text" value="' + esc(c.comment || c.name) + '" data-cn="' + esc(c.name) + '">启用<input type="checkbox" class="f-flag" data-flag="' + esc(c.name) + '"' + (c.name === 'isused' ? ' checked' : '') + '></div>').join('');
}
async function loadGenFields() {
  const v = $('gen-table-sel').value;
  $('gen-fields').innerHTML = '';
  if (!v) return;
  const [schema, table] = v.split('.');
  const d = await api('/api/table?schema=' + schema + '&table=' + table);
  $('gen-pk').value = d.pk || '';
  if (!$('gen-chinese').value) {
    const t = (typeof TABLES !== 'undefined' ? TABLES : []).find(x => x.schema === schema && x.table === table);
    $('gen-chinese').value = (t && t.comment) || table;
  }
  renderFieldList($('gen-fields'), d.columns, d.pk);
}
$('gen-kind').onchange = () => {
  const k = $('gen-kind').value;
  $('gen-child-box').style.display = k === 'parent_child' ? '' : 'none';
  if (k === 'parent_child') {
    const schema = ($('gen-table-sel').value || '').split('.')[0];
    const ptable = ($('gen-table-sel').value || '').split('.')[1];
    $('gen-child-sel').innerHTML = '<option value="">— 选择子表 —</option>' +
      TABLES.filter(t => t.schema === schema && t.table !== ptable)
        .map(t => '<option value="' + esc(t.schema) + '.' + esc(t.table) + '">' + esc(t.schema) + '.' + esc(t.table) + (t.comment ? '（' + esc(t.comment) + '）' : '') + '</option>').join('');
  }
};
async function loadChildFields() {
  const v = $('gen-child-sel').value;
  $('gen-child-fields').innerHTML = '';
  if (!v) return;
  const [schema, table] = v.split('.');
  const d = await api('/api/table?schema=' + schema + '&table=' + table);
  if (!$('gen-child-pk').value) $('gen-child-pk').value = d.pk || '';
  renderFieldList($('gen-child-fields'), d.columns, d.pk);
  /* 父子表（已有表）：关联字段下拉 = 子表全部列，默认选与父表主键同名的列 */
  const fkSel = $('gen-child-fk');
  if (fkSel && fkSel.tagName === 'SELECT') {
    const want = ($('gen-pk').value || '').trim();
    fkSel.innerHTML = d.columns.map(c => '<option value="' + esc(c.name) + '"' +
      (c.name === want ? ' selected' : '') + '>' + esc(c.name) + (c.comment ? '（' + esc(c.comment) + '）' : '') + '</option>').join('');
    if (![...fkSel.options].some(o => o.selected && o.value)) fkSel.value = d.columns[0] ? d.columns[0].name : '';
  }
}
$('gen-child-sel').onchange = loadChildFields;
/* 权限树 + ID 自动分配 */
let PERM = null, GAPIDS = null;
function updatePermHint() {
  if (!PERM) return;
  $('perm-hint').innerHTML = '挂载于：<b>' + esc(PERM.sysactionname) + '</b>（' + esc(PERM.idpath) +
    '）；本功能权限/菜单ID自动分配为 <b>' + (GAPIDS ? GAPIDS.menu_action : '…') + '</b>';
}
async function refreshIds() {
  GAPIDS = await api('/api/ids');
  $('act-add').value = GAPIDS.actions.add; $('act-edit').value = GAPIDS.actions.edit;
  $('act-merge').value = GAPIDS.actions.merge; $('act-del').value = GAPIDS.actions.del;
  $('act-undel').value = GAPIDS.actions.undel; $('act-query').value = GAPIDS.actions.query;
  updatePermHint();
}
let PERMNODES = [], SEARCH = '', EXPAND = null;
async function loadPermTree() {
  const d = await api('/api/perm/tree');
  PERMNODES = d.nodes.filter(n => String(n.isused) === '1');
  if (EXPAND === null) {
    EXPAND = new Set();
    PERMNODES.forEach(n => { if (n.idlevel <= 2) EXPAND.add(n.sysactionid); });
  }
  renderPermTree();
}
function renderPermTree() {
  const nodes = PERMNODES;
  const byId0 = {};
  nodes.forEach(n => { byId0[n.sysactionid] = n; });
  let vis = nodes, HITS = [];
  if (SEARCH) {
    const hitIds = new Set();
    nodes.forEach(n => {
      const hay = (n.sysactionname + ' ' + n.sysactionid + ' ' + n.idpath).toLowerCase();
      if (hay.includes(SEARCH)) {
        HITS.push(n);
        let cur = n;
        while (cur) { hitIds.add(cur.sysactionid); cur = byId0[cur.upid]; }
      }
    });
    vis = nodes.filter(n => hitIds.has(n.sysactionid));
  }
  window.__PERMHITS = HITS;
  if (EXPAND === null) {
    EXPAND = new Set();
    nodes.forEach(n => { if (n.idlevel <= 2) EXPAND.add(n.sysactionid); });
  }
  const kids = {};
  vis.forEach(n => { (kids[n.upid] = kids[n.upid] || []).push(n); });
  Object.values(kids).forEach(arr => arr.sort((x, y) => x.idpath < y.idpath ? -1 : 1));
  const descCount = n => {
    let c = 0;
    (kids[n.sysactionid] || []).forEach(k => { c += 1 + descCount(k); });
    return c;
  };
  let out = [];
  const walk = (n, prefix, isLast) => {
    const kidsArr = kids[n.sysactionid] || [];
    const hasKids = kidsArr.length > 0;
    const open = hasKids && (SEARCH || EXPAND.has(n.sysactionid));
    const cnt = hasKids && !open ? ' <span class="cnt">(' + descCount(n) + ')</span>' : '';
    out.push('<div class="perm-node' + (PERM && String(PERM.sysactionid) === String(n.sysactionid) ? ' sel' : '') +
      '" data-id="' + n.sysactionid + '">' + esc(prefix + (isLast ? '└─' : '├─')) +
      (hasKids ? '<span class="perm-tw" data-id="' + n.sysactionid + '">' + (open ? '▾' : '▸') + '</span>'
               : '<span class="perm-tw">·</span>') +
      ' ' + esc(n.sysactionname) + ' <span class="pid">' + n.sysactionid + '</span>' + cnt + '</div>');
    if (open) {
      kidsArr.forEach((k, idx) => walk(k, prefix + (isLast ? '　' : '│ '), idx === kidsArr.length - 1));
    }
  };
  let roots = (kids[0] || []).slice();
  if (!roots.length && vis.length) roots = [vis[0]];
  roots.forEach((r, idx) => walk(r, '', idx === roots.length - 1));
  $('perm-tree').innerHTML = out.join('') || '<div class="hint" style="padding:6px">无匹配权限节点</div>';
  [...$('perm-tree').querySelectorAll('.perm-node')].forEach(el => {
    el.onclick = (e) => {
      const tw = e.target.closest ? e.target.closest('.perm-tw') : null;
      if (tw && tw.dataset.id) {
        const id = parseInt(tw.dataset.id, 10);
        if (EXPAND.has(id)) EXPAND.delete(id); else EXPAND.add(id);
        renderPermTree();
        return;
      }
      PERM = nodes.find(x => String(x.sysactionid) === el.dataset.id);
      [...$('perm-tree').querySelectorAll('.perm-node')].forEach(x => x.classList.toggle('sel', x === el));
      updatePermHint();
    };
  });
  if (!PERM && !SEARCH) {
    const def = nodes.find(n => n.sysactionid === 9003) || nodes.find(n => n.idlevel === 2);
    if (def) { const el = $('perm-tree').querySelector('[data-id="' + def.sysactionid + '"]'); if (el) el.click(); }
  }
  if (SEARCH && HITS.length) {
    const el = $('perm-tree').querySelector('[data-id="' + HITS[0].sysactionid + '"]');
    if (el) el.scrollIntoView({block: 'center'});
  }
}
$('perm-search').oninput = () => { SEARCH = $('perm-search').value.trim().toLowerCase(); renderPermTree(); };
/* 系统管理（systeminfo） */
function genKey() {                       // 16 位随机密钥（匹配 prikey varchar(16)）
  const cs = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
  const a = new Uint8Array(16);
  crypto.getRandomValues(a);
  return [...a].map(b => cs[b % cs.length]).join('');
}
async function loadSystems() {
  const d = await api('/api/systems');
  $('gen-system').innerHTML = d.systems.map(s =>
    '<option value="' + s.systemid + '">' + s.systemid + ' ' + esc(s.systemname) + '</option>').join('');
  if (d.systems.some(s => s.systemid === 100)) $('gen-system').value = 100;
}
async function loadSysTable() {
  const d = await api('/api/systems');
  $('sys-list').querySelector('tbody').innerHTML = d.systems.map(s =>
    '<tr><td>' + s.systemid + '</td>' +
    '<td><input class="sname" data-id="' + s.systemid + '" value="' + esc(s.systemname) + '"></td>' +
    '<td><input type="checkbox" class="sused" data-id="' + s.systemid + '"' +
    (String(s.isused) === '1' ? ' checked' : '') + '></td>' +
    '<td><button class="mini" onclick="saveSys(' + s.systemid + ')">保存</button>' +
    '<button class="mini danger" onclick="delSys(' + s.systemid + ')">删除</button></td></tr>').join('');
}
window.saveSys = async function (sid) {
  const name = document.querySelector('.sname[data-id="' + sid + '"]').value.trim();
  const used = document.querySelector('.sused[data-id="' + sid + '"]').checked ? 1 : 0;
  try {
    await api('/api/system/update', {systemid: sid, systemname: name, isused: used});
    await loadSystems();
    await loadSysTable();
  } catch (e) { alert(e.message); }
};
window.delSys = async function (sid) {
  if (!confirm('确定删除系统 ' + sid + '？（该系统下存在权限节点时会被拒绝）')) return;
  try {
    await api('/api/system/delete', {systemid: sid});
    await loadSystems();
    await loadSysTable();
  } catch (e) { alert(e.message); }
};
$('btn-sys-mgr').onclick = () => {
  $('ns-id').value = ''; $('ns-sname').value = '';
  $('ns-prikey').value = genKey(); $('ns-algo').value = 1;
  msg('ns2-msg', '');
  loadSysTable();
  openModal('modal-sys');
};
$('btn-sys-create').onclick = async () => {
  const body = {systemname: $('ns-sname').value.trim(),
    prikey: $('ns-prikey').value.trim(), algorithm: +$('ns-algo').value || 1};
  if ($('ns-id').value.trim()) body.systemid = +$('ns-id').value.trim();
  try {
    const d = await api('/api/system/create', body);
    $('ns-sname').value = ''; $('ns-prikey').value = genKey();
    msg('ns2-msg', '已创建系统 ' + d.systemid + '（密钥已生成，请妥善保存）', true);
    await loadSystems();
    await loadSysTable();
  } catch (e) { msg('ns2-msg', e.message); }
};

function needPerm() { if (!PERM) { alert('请先在权限树中点选一个节点'); return false; } return true; }
$('btn-perm-edit2').onclick = () => { if (needPerm()) openPermEdit(PERM.sysactionid); };
$('btn-perm-add').onclick = () => {
  if (!needPerm()) return;
  $('pa-parent').textContent = '父节点：' + PERM.sysactionname + '（' + PERM.idpath + '）';
  $('pa-name').value = $('gen-chinese').value.trim();
  msg('pa-msg', '');
  openModal('modal-perm-add');
};
$('btn-perm-add-go').onclick = async () => {
  const body = {parent: PERM.sysactionid, name: $('pa-name').value.trim()};
  if (!body.name) { msg('pa-msg', '请填写节点名称'); return; }
  try {
    const d = await api('/api/perm/add', body);
    msg('pa-msg', '已创建权限节点 ' + d.sysactionid, true);
    await loadPermTree();
    PERM = PERMNODES.find(x => x.sysactionid === d.sysactionid);
    renderPermTree();
    updatePermHint();
    setTimeout(() => closeModal('modal-perm-add'), 700);
  } catch (e) { msg('pa-msg', e.message); }
};
$('btn-perm-del').onclick = async () => {
  if (!needPerm()) return;
  const sub = PERMNODES.filter(n => n.idpath === PERM.idpath || n.idpath.startsWith(PERM.idpath + '.')).length;
  if (!confirm('将停用节点「' + PERM.sysactionname + '」及其子树共 ' + sub + ' 个节点（isused=0），确定？')) return;
  try {
    const d = await api('/api/perm/delete', {sysactionid: PERM.sysactionid});
    alert('已停用 ' + d.affected + ' 个节点');
    PERM = null;
    await loadPermTree();
  } catch (e) { alert(e.message); }
};
async function permMove(direction) {
  if (!PERM) { alert('请先在权限树中点选一个节点'); return; }
  let target = null;
  if (SEARCH) {
    const hits = window.__PERMHITS || [];
    const i = hits.findIndex(n => n.sysactionid === PERM.sysactionid);
    if (i < 0) { alert('当前定位节点不在搜索结果中，请重新搜索'); return; }
    const j = direction === 'up' ? i - 1 : i + 1;
    if (j < 0) { alert('已是搜索结果第一个，无法上移'); return; }
    if (j >= hits.length) { alert('已是搜索结果最后一个，无法下移'); return; }
    target = hits[j];
  } else {
    const pp = PERM.idpath.slice(0, PERM.idpath.lastIndexOf('.'));
    const sibs = PERMNODES.filter(n => String(n.isused) === '1' && n.upid === PERM.upid &&
      n.idlevel === PERM.idlevel && (n.idpath === pp || n.idpath.indexOf(pp + '.') === 0))
      .sort((x, y) => x.idpath < y.idpath ? -1 : 1);
    const i = sibs.findIndex(n => n.sysactionid === PERM.sysactionid);
    const j = direction === 'up' ? i - 1 : i + 1;
    if (j < 0) { alert('已是同级第一个，无法上移'); return; }
    if (j >= sibs.length) { alert('已是同级最后一个，无法下移'); return; }
    target = sibs[j];
  }
  if (target.upid !== PERM.upid || target.idlevel !== PERM.idlevel) {
    alert('相邻结果与当前节点不同级，仅支持同级节点间移动'); return;
  }
  try {
    const d = await api('/api/perm/move', {sysactionid: PERM.sysactionid, target: target.sysactionid});
    if (!d.ok) { alert(d.detail); return; }
    const old = PERM.sysactionid;
    await loadPermTree();
    PERM = PERMNODES.find(x => x.sysactionid === old);
    renderPermTree();
    updatePermHint();
  } catch (e) { alert(e.message); }
}
$('btn-perm-up').onclick = () => permMove('up');
$('btn-perm-down').onclick = () => permMove('down');

/* 操作权限 actionid 联动：修改任一 ID 后，其后的 ID 顺延 +1 */
const ACT_IDS = ['act-add', 'act-edit', 'act-merge', 'act-del', 'act-undel', 'act-query'];
function syncActIds(fromIdx) {
  const start = parseInt($(ACT_IDS[fromIdx]).value, 10);
  if (isNaN(start)) return;
  for (let i = fromIdx; i < ACT_IDS.length; i++) {
    $(ACT_IDS[i]).value = start + (i - fromIdx);
  }
}
ACT_IDS.forEach((id, idx) => {
  $(id).addEventListener('input', () => syncActIds(idx));
  $(id).addEventListener('change', () => syncActIds(idx));
});
$('btn-suggest-ids').onclick = async () => {
  try { await refreshIds(); } catch (e) { msg('gen-msg', e.message); }
};

/* 修改权限（sysaction + actions 两表同事务更新） */
let PE_NODE = null, PE_OPS = [];
const VERB_MAP = {add: '新增', edit: '修改', merge: '合并', del: '删除', undel: '恢复', query: '查询'};
function verbOf(op) {
  const u = (op.action && op.action.actionurl) || '';
  const m = u.match(/_(add|edit|merge|del|undel|query)$/);
  return m ? VERB_MAP[m[1]] : '操作';
}
function openPermEdit(sid) {
  api('/api/perm/group?sysactionid=' + sid).then(d => {
    PE_NODE = d.menu; PE_OPS = d.ops;
    $('pe-id').textContent = d.menu.sysactionid;
    $('pe-path').textContent = 'idpath: ' + d.menu.idpath + '　层级: ' + d.menu.idlevel +
      '　系统: ' + d.menu.systemid;
    $('pe-name').value = d.menu.sysactionname || '';
    $('pe-isused').checked = String(d.menu.isused) === '1';
    $('pe-ops').querySelector('tbody').innerHTML = d.ops.map(op => {
      const aid = op.actionid || '';
      return '<tr data-sid="' + op.sysactionid + '" data-aid="' + aid + '">' +
        '<td>' + verbOf(op) + '</td><td>' + (aid || '-') + '</td>' +
        '<td><input class="op-name" value="' + esc(op.sysactionname) + '"></td>' +
        '<td><input class="op-url" value="' + esc(op.action ? op.action.actionurl : '') + '"></td>' +
        '<td><input class="op-desc" value="' + esc(op.action ? op.action.description : '') + '"></td>' +
        '<td><input type="checkbox" class="op-used"' + (String(op.isused) === '1' ? ' checked' : '') + '></td></tr>';
    }).join('') || '<tr><td colspan="6" class="hint">无操作权限节点</td></tr>';
    msg('pe-msg', '');
    openModal('modal-perm-edit');
  }).catch(e => alert(e.message));
}
$('btn-perm-save').onclick = async () => {
  const ops = [...$('pe-ops').querySelectorAll('tbody tr[data-sid]')].map(tr => {
    const o = {sysactionid: +tr.dataset.sid,
      sysactionname: tr.querySelector('.op-name').value.trim(),
      isused: tr.querySelector('.op-used').checked ? 1 : 0};
    if (tr.dataset.aid) {
      o.actionid = +tr.dataset.aid;
      o.action = {actionname: tr.querySelector('.op-name').value.trim(),
                  actionurl: tr.querySelector('.op-url').value.trim(),
                  description: tr.querySelector('.op-desc').value.trim()};
    }
    return o;
  });
  const body = {menu: {sysactionid: PE_NODE.sysactionid,
    sysactionname: $('pe-name').value.trim(), isused: $('pe-isused').checked ? 1 : 0}, ops};
  try {
    const d = await api('/api/perm/update_group', body);
    msg('pe-msg', '已保存（' + d.changed + ' 条更新，sysaction + actions 同事务）', true);
    await loadPermTree();
    setTimeout(() => closeModal('modal-perm-edit'), 700);
  } catch (e) { msg('pe-msg', e.message); }
};
function collectFields(el) {
  return [...el.querySelectorAll('.field-item')].filter(r => r.querySelector('input[type=checkbox]:not(.f-flag)').checked)
    .map(r => ({name: r.querySelector('[data-name]').dataset.name,
                cn: r.querySelector('[data-cn]').value.trim(),
                flag: r.querySelector('.f-flag').checked ? 1 : 0}));
}
function genBody() {
  const v = $('gen-table-sel').value;
  if (!v) { throw new Error('请先选择表'); }
  const [schema, table] = v.split('.');
  if (!PERM) { throw new Error('请在权限树中选择本功能挂载的权限节点'); }
  if (!GAPIDS) { throw new Error('权限ID尚未分配，请重试'); }
  const body = {schema, table, chinese: $('gen-chinese').value.trim(), pk: $('gen-pk').value.trim(),
          group: PERM.sysactionid, systemid: parseInt($('gen-system').value, 10) || 100,
          icon: $('gen-icon').value.trim(),
          menu_action: GAPIDS.menu_action,
          actions: {add: +$('act-add').value || GAPIDS.actions.add, edit: +$('act-edit').value || GAPIDS.actions.edit,
                    merge: +$('act-merge').value || GAPIDS.actions.merge, del: +$('act-del').value || GAPIDS.actions.del,
                    undel: +$('act-undel').value || GAPIDS.actions.undel, query: +$('act-query').value || GAPIDS.actions.query},
          kind: $('gen-kind').value,
          fields: collectFields($('gen-fields'))};
  if (body.kind === 'parent_child') {
    const cv = $('gen-child-sel').value;
    if (!cv) { throw new Error('父子表请选择子表'); }
    body.existing = 1;   /* 父表/子表均为已有表：不现场建表，仅生成父子联动模块 */
    body.child_table = cv.split('.')[1];
    body.child_pk = $('gen-child-pk').value.trim();
    body.child_fk = $('gen-child-fk').value;
    body.child_chinese = $('gen-child-cn').value.trim();
    /* 子表业务列由后端按子表实际列自动派生（排除主键/关联字段/结构列） */
  }
  return body;
}
$('btn-gen').onclick = async () => {
  try {
    await refreshIds();
    await loadGenFields();   /* 按当前表结构刷新字段列表（表结构变更后再次生成也能取到最新列） */
    if ($('gen-kind').value === 'parent_child') await loadChildFields();
    GEN = await api('/api/gen', genBody());
    $('gen-sql').textContent = GEN.sql;
    $('gen-mjs').textContent = GEN.modules_js;
    $('prev-frame').src = API + GEN.preview_url;
    document.querySelector('.stab[data-stab=sql]').click();
    $('btn-publish').disabled = false;
    msg('gen-msg', '生成完成（plpgsql）：SQL ' + GEN.sql.length + ' 字符。预览无误后点击发布。', true);
  } catch (e) { msg('gen-msg', e.message); }
};
$('btn-publish').onclick = async () => {
  if (!GEN) return;
  if (!confirm('将执行 SQL 到业务库并部署前端到 nginx，确认发布？')) return;
  try {
    const d = await api('/api/publish', genBody());
    const r = d.reload || {};
    msg('gen-msg', '发布成功：SQL 已应用，前端已部署到 ' + d.files.length + ' 处；函数缓存' +
      (r.ok ? '已热重载。' : '未能自动重载（' + (r.detail || '') + '），请到“已发布模块”页手动重载。'), true);
    GEN = null;
    $('btn-publish').disabled = true;
  } catch (e) { msg('gen-msg', e.message); }
};

/* ---------- 已发布 / 状态 ---------- */
async function loadPublished() {
  try {
    const d = await api('/api/published');
    $('pub-list').querySelector('tbody').innerHTML = d.modules.map(m =>
      '<tr><td><b>' + esc(m.table) + '</b></td><td>' + esc(m.chinese) + '</td><td>' + esc(m.group) +
      '</td><td>' + m.menu + '</td><td>' + esc(m.base) + '</td><td>' + esc(m.lang) + '</td><td>' + esc(m.ts || '—') +
      '</td></tr>').join('') || '<tr><td colspan="7" class="hint">暂无已发布模块</td></tr>';
  } catch (e) { alert(e.message); }
}
$('btn-pub-refresh').onclick = loadPublished;
$('btn-reload-fns').onclick = async () => {
  try {
    const d = await api('/api/reload');
    alert(d.ok ? '热重载成功：' + d.detail : '重载失败：' + d.detail);
  } catch (e) { alert(e.message); }
};
async function loadSys() {
  try {
    const d = await api('/api/info');
    $('db-chip').textContent = '业务库：' + d.db.name + '（' + (d.db.detect.detected ? '已探测' : '默认') + '）';
    $('nginx-chip').textContent = 'nginx worker：' + d.nginx.workers;
    $('sys-db').textContent = '版本: ' + d.db.version + '\n业务库: ' + d.db.name + ' @ ' + d.db.host + ':' + d.db.port +
      '\n用户: ' + d.db.user + '\n探测: ' + d.db.detect.basis;
    $('sys-nginx').textContent = 'worker 进程: ' + d.nginx.workers + '\n部署目录: ' + d.nginx.deploy_dir +
      '\n目录存在: ' + (d.nginx.deploy_exists ? '是' : '否');
    $('sys-admin').textContent = '数据目录: ' + d.admin.data_dir + '\n已生成模块: ' + (d.admin.lastgen.join(', ') || '无');
  } catch (e) { alert(e.message); }
}

/* ---------- 初始化 ---------- */
(async function init() {
  try {
    const info = await api('/api/info');
    $('db-chip').textContent = '业务库：' + info.db.name + (info.db.detect.detected ? '（已探测）' : '');
    $('nginx-chip').textContent = 'nginx worker：' + info.nginx.workers;
  } catch (e) {
    $('db-chip').textContent = '后端不可达：' + e.message;
  }
  try { await loadSchemas(); await loadTables(); await loadSystems(); await loadPermTree(); await refreshIds(); } catch (e) { console.warn(e); }
})();
