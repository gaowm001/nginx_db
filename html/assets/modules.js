'use strict';
/* NGXDB Admin — 模块配置。所有 actionid 均取自 sysinfo.actions 权威表（67行）。
 * P(id,title,group,menu,icon,base,pk,cols,fields,ops,dt)
 *   base: 接口前缀(实际调用 base+'_'+op)  pk: 该表主键字段名
 *   ops:  {add,edit,merge,del,undel} 各操作权限 actionid (查询无门控,表格自动加载)
 *   menu: 菜单项权限 actionid  dt: 详情子表类型('operpermission'/'rolepermission'/null)
 */
const GROUPS=[
  {id:'9001',t:'全系统管理',ic:'shield'},
  {id:'9002',t:'本系统管理',ic:'sitemap'},
  {id:'9003',t:'公共数据',ic:'map'}
];
const SOURCES={
  systeminfo:{base:'sysinfo/systeminfo',sv:'systemid',sl:'systemname'},
  orgtype:{base:'sysinfo/orgtype',sv:'orgtypeid',sl:'orgtypename'},
  sysorg:{base:'sysinfo/sysorg',sv:'sysorgid',sl:'sysorgname'},
  province:{base:'sysinfo/province',sv:'pid',sl:'province'}
};
const MODULES=[];
function P(id,title,group,menu,icon,base,pk,cols,fields,ops,dt,tree){MODULES.push({id:id,t:title,g:group,menu:menu,ic:icon,base:base,pk:pk,cols:cols,fields:fields,ops:ops||{},dt:dt||null,tree:!!tree});}

/* 全系统管理 (9001) */
P('sysaction','权限管理','9001',9100,'shield','sysinfo/sysaction','sysactionid',
  [{f:'sysactionid'},{f:'sysactionname'},{f:'systemname'},{f:'params',w:1},{f:'upid'},{f:'upname'},{f:'actionid'},{f:'isused',b:1}],
  [{f:'sysactionid',n:1},{f:'sysactionname',r:1},{f:'systemid',s:'systeminfo'},{f:'upid',tp:'sysaction'},{f:'actionid',n:1},{f:'params'},{f:'isdefault'},{f:'isused',sw:1}],
  {add:108,edit:109,merge:110,del:111,undel:112},null,1);
P('systeminfo','系统管理','9001',9103,'settings','sysinfo/systeminfo','systemid',
  [{f:'systemid'},{f:'systemname'},{f:'loginname'},{f:'prikey'},{f:'algorithm'},{f:'isused',b:1},{f:'createtime',w:1},{f:'updatetime',w:1},{f:'deltime',w:1}],
  [{f:'systemid',n:1},{f:'systemname',r:1},{f:'loginname'},{f:'prikey'},{f:'algorithm',n:1},{f:'isused',sw:1}],
  {add:101,edit:102,merge:103,del:104,undel:105},null);
P('operinfo','员工权限管理','9001',9104,'users','sysinfo/operinfo','operatorid',
  [{f:'operatorid'},{f:'operatorname'},{f:'accounts'},{f:'tokeninterval'},{f:'tokentype'},{f:'systemname'},{f:'isused',b:1}],
  [{f:'operatorid',n:1},{f:'operatorname',r:1},{f:'accounts',r:1},{f:'tokeninterval',n:1},{f:'tokentype',n:1},{f:'systemid',s:'systeminfo'},{f:'isused',sw:1}],
  {add:126,edit:127,merge:128,del:129,undel:130},null);
P('serverlog','系统日志查询','9001',107,'log','sysinfo/serverlog','logid',
  [{f:'logid'},{f:'logtime',w:1},{f:'clientip'},{f:'serverip'},{f:'funcname'},{f:'pckname'},{f:'operatorid'},{f:'params',w:1},{f:'res',w:1},{f:'content',w:1}],
  [],{},null);
P('actions','功能查询','9001',181,'code','sysinfo/actions','actionid',
  [{f:'actionid'},{f:'actionname'},{f:'actionurl',w:1},{f:'params',w:1},{f:'description',w:1},{f:'isused',b:1}],
  [],{},null);
P('errorcode','出错代码','9001',180,'alert','sysinfo/errorcode','errorcode',
  [{f:'errorcode'},{f:'message',w:1},{f:'primekey'},{f:'schema'},{f:'isused',b:1}],
  [],{},null);

/* 本系统管理 (9002) */
P('sysorg','部门管理','9002',9102,'sitemap','sysinfo/sysorg','sysorgid',
  [{f:'sysorgid'},{f:'sysorgname'},{f:'orgtype'},{f:'orgtypename'},{f:'description',w:1},{f:'upid'},{f:'upname'},{f:'isused',b:1}],
  [{f:'sysorgid',n:1},{f:'sysorgname',r:1},{f:'orgtype',s:'orgtype'},{f:'description'},{f:'upid',tp:'sysorg'},{f:'isused',sw:1}],
  {add:114,edit:115,merge:116,del:117,undel:118},null,1);
P('orgtype','部门类型管理','9002',9101,'tag','sysinfo/orgtype','orgtypeid',
  [{f:'orgtypeid'},{f:'orgtypename'},{f:'description',w:1},{f:'isused',b:1}],
  [{f:'orgtypeid',n:1},{f:'orgtypename',r:1},{f:'description'},{f:'isused',sw:1}],
  {add:120,edit:121,merge:122,del:123,undel:124},null);
P('roleinfo','角色管理','9002',9105,'tag','sysinfo/roleinfo','roleinfoid',
  [{f:'roleinfoid'},{f:'roleinfoname'},{f:'description',w:1},{f:'isused',b:1}],
  [{f:'roleinfoid',n:1},{f:'roleinfoname',r:1},{f:'description'},{f:'isused',sw:1}],
  {add:132,edit:133,merge:134,del:135,undel:136},'rolepermission');
P('operinfoorg','员工部门管理','9002',9106,'users','sysinfo/operinfoorg','operatorid',
  [{f:'operatorid'},{f:'operatorname'},{f:'accounts'},{f:'phone'},{f:'sex'},{f:'sysorgname'},{f:'nickname'},{f:'birthday'},{f:'memo',w:1},{f:'isused',b:1}],
  [{f:'operatorid',n:1},{f:'operatorname',r:1},{f:'accounts',r:1},{f:'phone'},{f:'sex'},{f:'sysorgid',tp:'sysorg'},{f:'nickname'},{f:'birthday'},{f:'memo',w:1},{f:'tokeninterval',n:1},{f:'tokentype',n:1},{f:'isused',sw:1}],
  {add:144,edit:145,merge:146,del:147,undel:148},null);
P('operinfopermission','员工权限分配','9002',9107,'key','sysinfo/operinfopermission','operatorid',
  [{f:'operatorid'},{f:'operatorname'},{f:'accounts'},{f:'isused',b:1}],
  [{f:'operatorid',n:1},{f:'operatorname',r:1},{f:'isused',sw:1}],
  {add:138,edit:139,merge:140,del:141,undel:142},'operpermission');

/* 公共数据 (9003) */
P('province','省','9003',9111,'map','sysinfo/province','pid',
  [{f:'pid'},{f:'province'},{f:'isused',b:1}],
  [{f:'pid',n:1},{f:'province',r:1},{f:'isused',sw:1}],
  {add:182,edit:183,merge:184,del:185,undel:186},null);
P('city','市','9003',9112,'pin','sysinfo/city','cid',
  [{f:'cid'},{f:'city'},{f:'pid'},{f:'province'},{f:'isused',b:1}],
  [{f:'cid',n:1},{f:'city',r:1},{f:'pid',s:'province'},{f:'isused',sw:1}],
  {add:188,edit:189,merge:190,del:191,undel:192},null);

/* 接口文档 (9001)：只读页(非 CRUD)，route() 特判走 renderGetHelp，
   数据来自 sysinfo.gethelp（权限节点 9113，挂在“系统 100”之下） */
P('gethelp','接口文档','9001',9113,'code','sysinfo/gethelp','',[],[],{},null);
