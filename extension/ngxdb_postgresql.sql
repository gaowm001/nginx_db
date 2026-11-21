--
-- PostgreSQL database dump
--

\restrict s5h1yJp7hfkcujfahXrb8hwC5ODsXL5gcxziIyLenTnnUgDjbaby6cHbravD9AK

-- Dumped from database version 18.6 (Ubuntu 18.6-0ubuntu0.26.04.1)
-- Dumped by pg_dump version 18.6 (Ubuntu 18.6-0ubuntu0.26.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: conn; Type: SCHEMA; Schema: -; Owner: conn
--

CREATE SCHEMA conn;


ALTER SCHEMA conn OWNER TO conn;

--
-- Name: crypto; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA crypto;


ALTER SCHEMA crypto OWNER TO postgres;

--
-- Name: gm; Type: SCHEMA; Schema: -; Owner: gm
--

CREATE SCHEMA gm;


ALTER SCHEMA gm OWNER TO gm;

--
-- Name: sysinfo; Type: SCHEMA; Schema: -; Owner: gm
--

CREATE SCHEMA sysinfo;


ALTER SCHEMA sysinfo OWNER TO gm;

--
-- Name: plpython3u; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpython3u WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpython3u; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpython3u IS 'PL/Python3U untrusted procedural language';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA crypto;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: actionupdate(); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.actionupdate() RETURNS boolean
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$BEGIN
  
	update sysinfo.sysaction set isused=1 where isused is null;
  with recursive tree as
  ( select sysactionid,upid,1 as idlevel,coalesce(actionid,sysactionid)::varchar as idpath,t1.systemid from sysinfo.sysaction t1 where upid=0
  union
  select t1.sysactionid,t1.upid,t2.idlevel+1,t2.idpath||'.'||coalesce(t1.actionid,t1.sysactionid),t1.systemid from sysinfo.sysaction t1 inner join tree t2 on t2.sysactionid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysaction t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysaction where isused<>0 group by upid,systemid) t2 on t1.sysactionid=t2.upid and t1.systemid=t2.systemid
  where t.sysactionid=t1.sysactionid;
  return true;
END
$$;


ALTER FUNCTION gm.actionupdate() OWNER TO gm;

COMMENT ON FUNCTION gm.actionupdate() IS '重建权限树（维护函数，非HTTP接口）：sysaction.isused 空值置1，并用递归CTE按 systemid 重算全部权限节点的 idpath/idlevel/idcount
返回：true';

--
-- Name: check_login(character varying, integer); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.check_login(p_token character varying, p_actionid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
	declare 
	  v_code varchar; 
		v_c int4;
		v_return json;
		v_token json;
		v_operatorid int;
		v_operjson json;
		v_systemid int;
		v_tokenstr varchar;
	BEGIN
    if p_actionid is null then return returnjson(0); end if;
		if p_token is null then return returnjson(100005); end if;
		select params into v_tokenstr from sysinfo.actions where actionid=101;
		v_token:=gm.aes_decrypt(p_token, v_tokenstr)::json;
		v_systemid:=(v_token->>'systemid')::integer;
		select row_to_json(t) into v_return from sysinfo.systeminfo t where v_systemid=systemid and isused=1 ;
		if v_return is null then return gm.returnjson(100012); end if;--子系统信息出错！
		v_token:=case (v_return->>'algorithm')::integer
		  when 1 then gm.aes_decrypt(v_token->>'token', (v_return->>'prikey')::text)
			when 2 then gm.aes_decrypt(v_token->>'token', (v_return->>'prikey')::text)
		end;		
    v_operatorid:=(v_token->>'operatorno')::integer;
		select array_to_json(array_agg(row_to_json(t))) into v_operjson from sysinfo.operinfo t where operatorid=v_operatorid and isused=1;
		if v_operjson is null or json_array_length(v_operjson)>1 then return gm.returnjson(100011); end if;--操作员信息出错！
		select count(*) into v_c from sysinfo.sysoper where operatorid=v_operatorid and systemid=v_systemid ;
		if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
		v_operjson:=v_operjson->0;
		case (v_operjson->>'tokentype')::integer 
		    when 1 then --1、单人登录
				  if v_operjson->>'tokenkey'!=p_token or (v_operjson->>'tokentime')::timestamp<now() then return gm.returnjson(100006);end if;--登录已失效！
				when 2 then --2、多人登录，不比较tokenkey
				  if (v_operjson->>'tokentime')::timestamp<now() then return gm.returnjson(100006);end if;--登录已失效！
				else 
				  return gm.returnjson(100006);--登录已失效！ 
		end case;
		perform set_config('app.operatorid', v_operatorid::text, false);--等保审计：向审计触发器提供当前操作者
		select count(*) into v_c from sysinfo.sysaction where coalesce(actionid,sysactionid) = p_actionid and systemid=v_systemid;
		if v_c=0 then return gm.returnjson(100008);end if; --'无此权限！';--10008
    select '.' || idpath || '.',isdefault into v_code,v_c from sysinfo.sysaction where coalesce(actionid,sysactionid) = p_actionid and systemid=v_systemid limit 1;
		if v_c=1 then return gm.returnjson(0,json_build_object('operatorid',v_operatorid,'systemid',v_systemid,'orgid',v_token->>'orgid'));end if;
    select count(*)
      into v_c
      from sysinfo.operpermission c left join sysinfo.sysaction c1 on c1.sysactionid=c.sysactionid and c1.systemid=v_systemid
     where c.operatorid = v_operatorid and c1.systemid=v_systemid
       and position( '.' || coalesce(c1.actionid,c1.sysactionid) || '.' in v_code) > 0
       and permissiontype = 1;
    if v_c > 0 then
			update sysinfo.operinfo set tokentime=now()+ (interval '1 minute')*tokeninterval where operatorid=v_operatorid;
      return gm.returnjson(0,json_build_object('operatorid',v_operatorid,'systemid',v_systemid,'orgid',v_token->>'orgid'));
    end if;
		with recursive c as (
	select c3.sysactionid,c3.permissiontype from sysinfo.operpermission c2 left join sysinfo.rolepermission c3 on c2.sysactionid=c3.roleinfoid left join sysinfo.sysaction c4 on c2.sysactionid=c4.sysactionid where c2.permissiontype=2 and c2.operatorid=v_operatorid and c4.systemid=v_systemid
	union 
	select c3.sysactionid,c3.permissiontype from c left join sysinfo.rolepermission c3 on c.sysactionid=c3.roleinfoid left join sysinfo.sysaction c4 on c3.sysactionid=c4.sysactionid where c.permissiontype=2 and c4.systemid=v_systemid
	)
    select count(*)
      into v_c
      from c left join sysinfo.sysaction c1 on c1.sysactionid=c.sysactionid and c1.systemid=v_systemid
     where permissiontype = 1 and position( '.' || coalesce(c1.actionid,c1.sysactionid) || '.' in v_code) > 0;
    if v_c > 0 then
			update sysinfo.operinfo set tokentime=now()+ (interval '1 minute')*tokeninterval where operatorid=v_operatorid;
      return gm.returnjson(0,json_build_object('operatorid',v_operatorid,'systemid',v_systemid,'orgid',v_token->>'orgid'));
    end if;
    return gm.returnjson(100008); --'无此权限！';--10008
  exception when others then return gm.returnjson(100005);--非法登录！
	
END
$$;


ALTER FUNCTION gm.check_login(p_token character varying, p_actionid integer) OWNER TO gm;

COMMENT ON FUNCTION gm.check_login(p_token character varying, p_actionid integer) IS '校验登录态与操作权限（全部业务函数的统一鉴权入口）
p_token:登录返回的AES加密token
p_actionid:所需权限点编号；为空时跳过权限校验直接放行
流程：两层AES解密token(外层actions[101]密钥，内层子系统prikey)→校验子系统/操作员有效性与登录态(单人比较tokenkey/多人仅校验有效期)→set_config(app.operatorid)供审计触发器取操作者→按直接授权或角色递归授权匹配权限点并顺延tokentime
错误码：100005非法登录、100012子系统信息出错、100011操作员信息出错、100006登录已失效、100008无此权限
返回：{"errorcode":0,"message":"执行成功！","info":{"operatorid":操作员id,"systemid":子系统id,"orgid":机构id}}';

--
-- Name: gettoken(); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.gettoken() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
	declare v_cur record;v_return jsonb;
begin
  for v_cur in select appid,params from sysinfo.appparams where typeid=101 loop
	  v_return:=gm.http_get('https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&'||v_cur.params);
		if v_return?'access_token' then 
		  update sysinfo.appparams set accesstoken=v_return->>'access_token',tokentime=now() where appid=v_cur.appid;
		end if;
	end loop;
	--create event gettoken on schedule 1 hour do gm.gettoken;
	-- 调度由外部定时器（cron/pg_cron 等）驱动；库内不再依赖 pkg_service 扩展
	-- （失败重试间隔建议 5 分钟，即 1/288 天）
  return;
END$$;


ALTER FUNCTION gm.gettoken() OWNER TO gm;

COMMENT ON FUNCTION gm.gettoken() IS '定时任务(job 101)：刷新微信公众号/小程序 access_token
遍历 sysinfo.appparams(typeid=101) 调微信接口获取 access_token，更新 accesstoken/tokentime；调度由外部定时器（cron/pg_cron）驱动，失败建议 5 分钟后重试';

--
-- Name: gettokene(); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.gettokene() RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
	declare v_cur record;v_return jsonb;
begin
  for v_cur in select appid,params from sysinfo.appparams where typeid=101 loop
	  v_return:=gm.http_get('https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&'||v_cur.params);
		if v_return?'access_token' then 
		  update sysinfo.appparams set accesstoken=v_return->>'access_token',tokentime=now() where appid=v_cur.appid;
		end if;
	end loop;
	--create event gettoken on schedule 1 hour do gm.gettoken;
	-- 调度由外部定时器（cron/pg_cron 等）驱动；库内不再依赖 pkg_service 扩展
	-- （成功后下一轮间隔 24 小时，即 1.0/24 天）
return;
END$$;


ALTER FUNCTION gm.gettokene() OWNER TO gm;

COMMENT ON FUNCTION gm.gettokene() IS '定时任务(job 100)：刷新微信公众号/小程序 access_token（24小时一轮）
遍历 sysinfo.appparams(typeid=101) 调微信接口获取 access_token，更新 accesstoken/tokentime；调度由外部定时器驱动';

--
-- Name: http_get(character varying); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.http_get(p_https character varying) RETURNS character varying
    LANGUAGE plpython3u SECURITY DEFINER
    AS $$

	import requests
		
	rv=requests.get(p_https,verify=False)
	plpy.notice(rv.text)
	return rv.text
$$;


ALTER FUNCTION gm.http_get(p_https character varying) OWNER TO gm;

COMMENT ON FUNCTION gm.http_get(p_https character varying) IS '服务端HTTP GET请求（plpython3u实现，仅供库内定时任务/内部逻辑调用，不对外暴露）
p_https:完整URL（verify=False，不校验TLS证书，适配内网自签证书）
返回：响应正文文本';

--
-- Name: http_post(character varying, character varying, character varying); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.http_post(p_https character varying, p_content character varying, p_contenttype character varying) RETURNS character varying
    LANGUAGE plpython3u SECURITY DEFINER
    AS $$

	import requests
	if p_contenttype:	
	  rv=requests.post(p_https,p_content,headers={'content-type':p_contenttype},verify=False)
	else:
	  rv=requests.post(p_https,p_content,verify=False)
	plpy.notice(rv.text)
	return rv.text
$$;


ALTER FUNCTION gm.http_post(p_https character varying, p_content character varying, p_contenttype character varying) OWNER TO gm;

COMMENT ON FUNCTION gm.http_post(p_https character varying, p_content character varying, p_contenttype character varying) IS '服务端HTTP POST请求（plpython3u实现，仅供库内定时任务/内部逻辑调用，不对外暴露）
p_https:完整URL
p_content:请求体
p_contenttype:Content-Type请求头，可为空
返回：响应正文文本';

--
-- Name: returnjson(integer, json); Type: FUNCTION; Schema: gm; Owner: gm
--

CREATE FUNCTION gm.returnjson(p_code integer, p_jsonvar json DEFAULT NULL::json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
	 declare v_message varchar;v_c int;
	BEGIN
		select count(*) into v_c from sysinfo.errorcode where errorcode=p_code;
		
		if v_c=0 or v_c>1 then return json_build_object('errorcode',p_code,'message','未知错误'); end if;
		select message into v_message from sysinfo.errorcode where errorcode=p_code;
		if p_jsonvar is null then 
		   return json_build_object('errorcode',p_code,'message',v_message);
		else
		   return json_build_object('errorcode',p_code,'message',v_message,'info',p_jsonvar);
		end if;
	END
$$;


ALTER FUNCTION gm.returnjson(p_code integer, p_jsonvar json) OWNER TO gm;

COMMENT ON FUNCTION gm.returnjson(p_code integer, p_jsonvar json) IS '统一返回结构：按 sysinfo.errorcode 表把错误码翻译为提示语
p_code:错误码（0成功，其余见 errorcode 表，如100005非法登录、100008无此权限）
p_jsonvar:业务数据JSON，可为空
返回：p_jsonvar为空时 {"errorcode":码,"message":提示}；否则 {"errorcode":码,"message":提示,"info":业务数据}；错误码在表中不存在或重复时 message 为 未知错误';

--
-- Name: _ensure_roles(); Type: FUNCTION; Schema: gm; Owner: postgres
--

CREATE FUNCTION gm._ensure_roles() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'conn') THEN
    CREATE ROLE conn PASSWORD 'CHANGE_ME';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gm') THEN
    CREATE ROLE gm PASSWORD 'CHANGE_ME';
  END IF;
END;
$$;


ALTER FUNCTION gm._ensure_roles() OWNER TO postgres;

--
-- Name: actions_query(character varying, integer, integer, integer, integer, character varying, character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.actions_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_actionid integer, p_actionname character varying, p_description character varying, p_params character varying, p_actionurl character varying, p_code character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,180);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.actionid,t1.actionname,t1.description,t1.params,t1.actionurl,t1.code from sysinfo.actions t1 where  (p_isused is null or p_isused=t1.isused) and (p_actionid is null or p_actionid=t1.actionid) and (p_actionname is null or position(p_actionname in t1.actionname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_params is null or position(p_params in t1.params )>0) and (p_actionurl is null or position(p_actionurl in t1.actionurl )>0) and (p_code is null or position(p_code in t1.code )>0) order by t1.actionid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.actions t1 where  (p_isused is null or p_isused=t1.isused) and (p_actionid is null or p_actionid=t1.actionid) and (p_actionname is null or position(p_actionname in t1.actionname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_params is null or position(p_params in t1.params )>0) and (p_actionurl is null or position(p_actionurl in t1.actionurl )>0) and (p_code is null or position(p_code in t1.code )>0);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.actionid,t1.actionname,t1.description,t1.params,t1.actionurl,t1.code from sysinfo.actions t1 where  (p_isused is null or p_isused=t1.isused) and (p_actionid is null or p_actionid=t1.actionid) and (p_actionname is null or position(p_actionname in t1.actionname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_params is null or position(p_params in t1.params )>0) and (p_actionurl is null or position(p_actionurl in t1.actionurl )>0) and (p_code is null or position(p_code in t1.code )>0) order by t1.actionid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.actions_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_actionid integer, p_actionname character varying, p_description character varying, p_params character varying, p_actionurl character varying, p_code character varying) OWNER TO gm;

--
-- Name: FUNCTION actions_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_actionid integer, p_actionname character varying, p_description character varying, p_params character varying, p_actionurl character varying, p_code character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.actions_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_actionid integer, p_actionname character varying, p_description character varying, p_params character varying, p_actionurl character varying, p_code character varying) IS '查询权限名
isused:是否使用
actionid:权限编号
actionname:权限名
description:说明
params:参数
actionurl:权限路径
code:编码
返回:{"total":总记录数,"rows":[{"isused":使用,"actionid":权限编号,"actionname":权限名,"description":说明,"params":参数,"actionurl":权限路径,"code":编码}]}';


--
-- Name: aes_decrypt(text, text); Type: FUNCTION; Schema: gm; Owner: postgres
--

CREATE FUNCTION gm.aes_decrypt(p_text text, p_key text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT SECURITY DEFINER
    AS $$
DECLARE
    v_out text;
BEGIN
    -- 1) armored / raw PGP text
    BEGIN
        v_out := crypto.pgp_sym_decrypt(p_text::bytea, p_key);
        RETURN v_out;
    EXCEPTION WHEN others THEN v_out := NULL; END;

    -- 2) base64-encoded PGP binary
    BEGIN
        v_out := crypto.pgp_sym_decrypt(decode(p_text, 'base64'), p_key);
        RETURN v_out;
    EXCEPTION WHEN others THEN v_out := NULL; END;

    -- 3) base64 -> utf8 armored text
    BEGIN
        v_out := crypto.pgp_sym_decrypt(
            convert_from(decode(p_text, 'base64'), 'UTF8')::bytea, p_key);
        RETURN v_out;
    EXCEPTION WHEN others THEN v_out := NULL; END;

    -- undecryptable input: return NULL instead of raising
    RETURN NULL;
END;
$$;


ALTER FUNCTION gm.aes_decrypt(p_text text, p_key text) OWNER TO postgres;

--
-- Name: aes_encrypt(text, text); Type: FUNCTION; Schema: gm; Owner: postgres
--

CREATE FUNCTION gm.aes_encrypt(p_text text, p_key text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE STRICT SECURITY DEFINER
    AS $$
BEGIN
    -- PGP binary ciphertext -> base64 -> strip CR/LF so the token stays on a
    -- single line %.  (OpenGauss gs_encrypt returned a similarly opaque
    --  one-line text; keeping it single-line makes tokenkey comparison in
    -- www check_login() byte-exact.)
    RETURN replace(
             replace(
               replace(encode(crypto.pgp_sym_encrypt(p_text, p_key), 'base64'), E'\n', ''),
             E'\r', ''),
           E'\r\n', '');
END;
$$;


ALTER FUNCTION gm.aes_encrypt(p_text text, p_key text) OWNER TO postgres;

--
-- Name: city_add(character varying, integer, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.city_add(p_token character varying, p_pid integer, p_cid integer, p_city character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,188);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_pid is null then return gm.returnjson(100034); end if;--市信息出错！
  select count(*) into v_c from public.province where p_pid=pid;
  if v_c=0 then return gm.returnjson(100033);end if;--省信息出错！
  if p_cid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('public.city_cid_seq');
      select count(*) into v_c from public.city where cid=v_id;
    end loop;
  else
    select count(*) into v_c from public.city where cid=p_cid;
    if v_c>0 then return gm.returnjson(100034); end if;--市信息出错！
    v_id:=p_cid;
  end if;
  insert into public.city( pid,cid,city,isused) values(p_pid,v_id,p_city,1 );
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION public.city_add(p_token character varying, p_pid integer, p_cid integer, p_city character varying) OWNER TO gm;

--
-- Name: FUNCTION city_add(p_token character varying, p_pid integer, p_cid integer, p_city character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.city_add(p_token character varying, p_pid integer, p_cid integer, p_city character varying) IS '新增市
pid:省id
cid:市id
city:市名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: city_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.city_del(p_token character varying, p_cid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,188);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from public.city where cid=p_cid;
  if v_c=0 then return gm.returnjson(100034); end if;--市信息出错！
  update public.city set isused=0 where cid=p_cid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION public.city_del(p_token character varying, p_cid integer) OWNER TO gm;

--
-- Name: FUNCTION city_del(p_token character varying, p_cid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.city_del(p_token character varying, p_cid integer) IS '删除市
cid:id';


--
-- Name: city_edit(character varying, integer, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.city_edit(p_token character varying, p_pid integer, p_cid integer, p_city character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,188);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_pid is null then return gm.returnjson(100034); end if;--市信息出错！
  select count(*) into v_c from public.province where p_pid=pid;
  if v_c=0 then return gm.returnjson(100033);end if;--省信息出错！
  v_id:=p_cid;
  select count(*) into v_c from public.city where cid=p_cid;
  if v_c=0 then return gm.returnjson(100034); end if;--市信息出错！
  update public.city set pid=p_pid,city=p_city where cid=p_cid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION public.city_edit(p_token character varying, p_pid integer, p_cid integer, p_city character varying) OWNER TO gm;

--
-- Name: FUNCTION city_edit(p_token character varying, p_pid integer, p_cid integer, p_city character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.city_edit(p_token character varying, p_pid integer, p_cid integer, p_city character varying) IS '修改市
pid:省id
cid:市id
city:市名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: city_merge(character varying, integer, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.city_merge(p_token character varying, p_pid integer, p_cid integer, p_city character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,188);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_pid is null then return gm.returnjson(100034); end if;--市信息出错！
  select count(*) into v_c from public.province where p_pid=pid;
  if v_c=0 then return gm.returnjson(100033);end if;--省信息出错！
  if p_cid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('public.city_cid_seq');
      select count(*) into v_c from public.city where cid=v_id;
    end loop;
  else
    v_id:=p_cid;
  end if;
  with "te" as (update public.city set pid=p_pid,city=p_city  where cid=v_id returning *)
  insert into public.city ( pid,cid,city,isused) select p_pid,v_id,p_city,1  where (select count(*) from te) = 0;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION public.city_merge(p_token character varying, p_pid integer, p_cid integer, p_city character varying) OWNER TO gm;

--
-- Name: FUNCTION city_merge(p_token character varying, p_pid integer, p_cid integer, p_city character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.city_merge(p_token character varying, p_pid integer, p_cid integer, p_city character varying) IS '合并市
pid:省id
cid:市id
city:市名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: city_query(character varying, integer, integer, integer, integer, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.city_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_cid integer, p_city character varying, p_isused integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,188);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.pid,t1.cid,t1.city,t1.isused,t2.province from public.city t1 left join public.province t2 on t2.pid=t1.pid where  (p_pid is null or p_pid=t1.pid) and (p_cid is null or p_cid=t1.cid) and (p_city is null or position(p_city in t1.city )>0) and (p_isused is null or p_isused=t1.isused) order by t1.cid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from public.city t1 where  (p_pid is null or p_pid=t1.pid) and (p_cid is null or p_cid=t1.cid) and (p_city is null or position(p_city in t1.city )>0) and (p_isused is null or p_isused=t1.isused);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.pid,t1.cid,t1.city,t1.isused,t2.province from public.city t1 left join public.province t2 on t2.pid=t1.pid where  (p_pid is null or p_pid=t1.pid) and (p_cid is null or p_cid=t1.cid) and (p_city is null or position(p_city in t1.city )>0) and (p_isused is null or p_isused=t1.isused) order by t1.cid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION public.city_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_cid integer, p_city character varying, p_isused integer) OWNER TO gm;

--
-- Name: FUNCTION city_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_cid integer, p_city character varying, p_isused integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.city_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_cid integer, p_city character varying, p_isused integer) IS '查询市
pid:省id
cid:市id
city:市名
isused:是否使用
返回:{"total":总记录数,"rows":[{"pid":省id,"cid":市id,"city":市名,"isused":是否使用,"province":省名}]}';


--
-- Name: city_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.city_undel(p_token character varying, p_cid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,189);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from public.city where cid=p_cid;
  if v_c=0 then return gm.returnjson(100034); end if;--市信息出错！
  update public.city set isused=1 where cid=p_cid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION public.city_undel(p_token character varying, p_cid integer) OWNER TO gm;

--
-- Name: FUNCTION city_undel(p_token character varying, p_cid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.city_undel(p_token character varying, p_cid integer) IS '恢复市
cid:id';


--
-- Name: errorcode_query(character varying, integer, integer, character varying, integer, character varying, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.errorcode_query(p_token character varying, p_rows integer, p_page integer, p_message character varying, p_errorcode integer, p_primekey character varying, p_isused integer, p_schema character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,181);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.message,t1.errorcode,t1.primekey,t1.isused,t1.schema from sysinfo.errorcode t1 where  (p_message is null or position(p_message in t1.message )>0) and (p_errorcode is null or p_errorcode=t1.errorcode) and (p_primekey is null or position(p_primekey in t1.primekey )>0) and (p_isused is null or p_isused=t1.isused) and (p_schema is null or position(p_schema in t1.schema )>0) order by t1.errorcode) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.errorcode t1 where  (p_message is null or position(p_message in t1.message )>0) and (p_errorcode is null or p_errorcode=t1.errorcode) and (p_primekey is null or position(p_primekey in t1.primekey )>0) and (p_isused is null or p_isused=t1.isused) and (p_schema is null or position(p_schema in t1.schema )>0);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.message,t1.errorcode,t1.primekey,t1.isused,t1.schema from sysinfo.errorcode t1 where  (p_message is null or position(p_message in t1.message )>0) and (p_errorcode is null or p_errorcode=t1.errorcode) and (p_primekey is null or position(p_primekey in t1.primekey )>0) and (p_isused is null or p_isused=t1.isused) and (p_schema is null or position(p_schema in t1.schema )>0) order by t1.errorcode limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.errorcode_query(p_token character varying, p_rows integer, p_page integer, p_message character varying, p_errorcode integer, p_primekey character varying, p_isused integer, p_schema character varying) OWNER TO gm;

--
-- Name: FUNCTION errorcode_query(p_token character varying, p_rows integer, p_page integer, p_message character varying, p_errorcode integer, p_primekey character varying, p_isused integer, p_schema character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.errorcode_query(p_token character varying, p_rows integer, p_page integer, p_message character varying, p_errorcode integer, p_primekey character varying, p_isused integer, p_schema character varying) IS '查询出错代码
message:信息
errorcode:出错代码
primekey:主键
isused:是否使用
schema:模式
返回:{"total":总记录数,"rows":[{"message":信息,"errorcode":出错代码,"primekey":主键,"isused":使用,"schema":模式}]}';


--
-- Name: login(character varying, character varying, inet, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.login(p_loginname character varying, p_pass character varying, p_ip inet, p_system character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
	declare v_c int;v_token varchar;v_pass varchar;v_return json;v_systemid int;v_keystr varchar;v_orgjson json;v_operatorid int;
	begin		
	  if p_loginname is null then 
		  return gm.returnjson(100009) ;
		end if; --账号不能为空
		if p_pass is null then 
	    return gm.returnjson(100010);
		end if; --密码不能为空
		select count(*) into v_c from public.loginlog where (accounts=p_loginname) and logintime>now()-interval '10 minute' ;
		if v_c>5 then return gm.returnjson(100002); end if;--登录错误超过5次，帐号锁定10分钟！
		select count(*) into v_c from public.loginlog where (accounts=p_loginname) and logintime>now()-interval '3 hour' ;
		if v_c>10 then return gm.returnjson(100003); end if;--登录错误超过10次，帐号锁定3小时！
		if p_system is null then 
		  v_systemid=100;
		else
		  select count(*) into v_c from sysinfo.systeminfo where loginname=p_system and isused=1;
			if v_c=1 then 
			  select systemid into v_systemid from sysinfo.systeminfo where loginname=p_system;
			else 
			  insert into public.loginlog(logid,logintime,operatorno,ip,accounts,systemid) values(nextval('public.loginlog_logid_seq'),now(),null,p_ip,p_loginname,v_systemid);
			  return gm.returnjson(100001); --帐号密码错误
			end if;
		end if;
		select count(*) into v_c from sysinfo.operinfo where accounts=p_loginname and pass=crypto.crypt(p_pass,pass) and isused=1;
		if v_c<>1 then
		  insert into public.loginlog(logid,logintime,operatorno,ip,accounts,systemid) values(nextval('public.loginlog_logid_seq'),now(),(v_return->>'operatorid')::integer,p_ip,p_loginname,v_systemid);
      return gm.returnjson(100001);--帐号密码错误
		end if;
		select operatorid into v_operatorid from sysinfo.operinfo where accounts=p_loginname and pass=crypto.crypt(p_pass,pass) and isused=1;
		select count(*) into v_c from sysinfo.sysoper where operatorid=v_operatorid and systemid=v_systemid;
		  if v_c=0 then
		    insert into public.loginlog(logid,logintime,operatorno,ip,accounts,systemid) values(nextval('public.loginlog_logid_seq'),now(),(v_return->>'operatorid')::integer,p_ip,p_loginname,v_systemid);
        return gm.returnjson(100001);--帐号密码错误
			end if;
		select array_to_json(array_agg(row_to_json(t))) into v_orgjson from (select t1.sysorgid,t1.sysorgname from sysinfo.sysorg t1 left join sysinfo.sysoperorg t2 on t1.sysorgid=t2.sysorgid where t2.operatorid=v_operatorid and t1.systemid=v_systemid ) t;
		select array_to_json(array_agg(row_to_json(t))) into v_return from (select t2.operatorid,t3.systemid,t2.operatorname,t2.sex,t2.phone,t2.memo,t2.mycode,t2.upcode,t2.headimgurl,t2.nickname,t2.tokeninterval,t3.systemname,t3.algorithm,t3.prikey from sysinfo.operinfo t2,sysinfo.systeminfo t3 where t2.operatorid=v_operatorid and t3.systemid=v_systemid) t; 
		v_return:=v_return->0;
		select params into v_keystr from sysinfo.actions where actionid=101;
		v_token:=case (v_return->>'algorithm')::integer
		  when 1 then gm.aes_encrypt(json_build_object('systemid',v_return->'systemid','token',gm.aes_encrypt(json_build_object('operatorno',v_return->'operatorid','tokentime',now()::varchar,'tokeninterval',v_return->>'tokeninterval')::text, (v_return->>'prikey')::text))::text,v_keystr)
			when 2 then gm.aes_encrypt(json_build_object('systemid',v_return->'systemid','token',gm.aes_encrypt(json_build_object('operatorno',v_return->'operatorid','systemid',v_return->'systemid','tokentime',now()::varchar,'tokeninterval',v_return->>'tokeninterval')::text, (v_return->>'prikey')::text))::text,v_keystr)
		end;
  update sysinfo.operinfo set tokenkey=v_token,tokentime = now()::timestamp + (interval '1 minute')*tokeninterval where operatorid = (v_return->>'operatorid')::integer;
select json_build_object('token',v_token,'operator',json_build_object('sex',v_return->>'sex','memo',v_return->>'memo','org',v_return->>'org','mycode',v_return->>'mycode','upcode',v_return->>'upcode','birthday',v_return->>'birthday','headimgurl',v_return->>'headimgurl','operatorname',v_return->>'operatorname','systemname',v_return->>'systemname','org',v_orgjson),'actions',array_to_json(array_agg(actionid))) into v_return from (
select distinct coalesce(t1.sysactionid,t1.actionid) actionid from sysinfo.sysaction t1,(
select distinct '.'||idpath||'.' actioncode from (
with recursive c as (
	select c3.sysactionid,c3.permissiontype from sysinfo.operpermission c2 left join sysinfo.rolepermission c3 on c2.sysactionid=c3.roleinfoid left join sysinfo.sysaction c4 on c2.sysactionid=c4.sysactionid where c2.permissiontype=2 and c2.operatorid=(v_return->>'operatorid')::integer and c4.systemid=(v_return->>'systemid')::integer
	union 
	select c3.sysactionid,c3.permissiontype from c left join sysinfo.rolepermission c3 on c.sysactionid=c3.roleinfoid left join sysinfo.sysaction c4 on c3.sysactionid=c4.sysactionid where c.permissiontype=2 and c4.systemid=(v_return->>'systemid')::integer
	) select sysactionid from c
  union
	select t1.sysactionid from sysinfo.operpermission t1 left join sysinfo.sysaction c4 on t1.sysactionid=c4.sysactionid where t1.operatorid=(v_return->>'operatorid')::integer and t1.permissiontype=1 and c4.systemid=(v_return->>'systemid')::integer
) t1
left join sysinfo.sysaction t2 on t1.sysactionid=t2.sysactionid where  systemid=(v_return->>'systemid')::integer) t2
where position('.'||t1.idpath||'.' in t2.actioncode)>0 
   or position(t2.actioncode in '.'||t1.idpath||'.')>0
	 order by actionid) t;
	 
		
	
	return gm.returnjson(0,v_return);

END$$;


ALTER FUNCTION sysinfo.login(p_loginname character varying, p_pass character varying, p_ip inet, p_system character varying) OWNER TO gm;

--
-- Name: FUNCTION login(p_loginname character varying, p_pass character varying, p_ip inet, p_system character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.login(p_loginname character varying, p_pass character varying, p_ip inet, p_system character varying) IS 'sysinfo.login
loginname:登录名
pass:密码
system:系统名,为空表示顶级系统';


--
-- Name: loginaccount(character varying, character varying, character varying, integer, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.loginaccount(p_loginname character varying, p_system character varying, p_ip character varying, p_accounttype integer, p_appid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
	declare 
		v_c numeric;v_isused integer;v_operatorid integer;v_token varchar;v_pass varchar;	v_tokentime timestamp;v_return json;v_systemid int;v_orgjson json;v_keystr varchar;
	begin		
		if p_system is null then 
		  v_systemid=100;
		else
		  select count(*) into v_c from sysinfo.systeminfo where loginname=p_system and isused=1;
			if v_c=1 then 
			  select systemid into v_systemid from sysinfo.systeminfo where loginname=p_system;
			else 
			  insert into public.loginlog(logid,logintime,operatorno,ip,accounts,systemid) values(nextval('public.loginlog_logid_seq'),now(),null,p_ip::inet,p_loginname,v_systemid);
			  return gm.returnjson(100001); --帐号密码错误
			end if;
		end if;
		case p_accounttype
		  when 101 then--微信小程序		
			  select count(*) into v_c from sysinfo.appparams where appid=p_appid and isused=1;
				if v_c=0 then return gm.returnjson(100001);end if; --帐号密码错误
				select 'https://api.weixin.qq.com/sns/jscode2session?grant_type=authorization_code&'||params||'&js_code='||p_loginname into v_pass from sysinfo.appparams where appid=p_appid;				
				--return json_build_object('message',v_pass);
				v_return:=gm.http_get(v_pass);
				if not v_return::jsonb?'openid' then return gm.returnjson(100001);end if; --帐号密码错误
				v_pass=v_return->>'openid';
				select count(*) into v_c from sysinfo.operaccounts where accounts=v_pass and typeid=101 and isused<>0 and isused=1;
				if v_c=0 then return gm.returnjson(100001);end if; --帐号密码错误
				select operatorid into v_operatorid from sysinfo.operaccounts where accounts=v_pass and typeid=101 and isused=1 ;
				if v_return::jsonb?'unionid' then
				  update sysinfo.operaccounts set unionid=v_return->>'unionid' where operatorid=v_operatorid and appid=p_appid and typeid=101;					        
        end if;
			else 
				return gm.returnjson(100001); --帐号密码错误
		end case; 
		select count(*) into v_c from sysinfo.sysorg t1 left join sysinfo.sysoperorg t2 on t1.sysorgid=t2.sysorgid where t2.operatorid=v_operatorid and t1.systemid=v_systemid and t1.isused=1 and t2.isused=1;
		if v_c=0 then 
		select count(*) into v_c from sysinfo.sysoper where operatorid=v_operatorid and systemid=v_systemid;
		  if v_c=0 then 
		    insert into public.loginlog values(nextval('public.loginlog_logid_seq'),now(),(v_return->>'operatorid')::integer,p_pass,p_ip::inet,p_loginname,v_systemid);
        return gm.returnjson(100001);--帐号密码错误
			end if;
		end if;
		select array_to_json(array_agg(row_to_json(t))) into v_orgjson from (select t1.sysorgid,t1.sysorgname from sysinfo.sysorg t1 left join sysinfo.sysoperorg t2 on t1.sysorgid=t2.sysorgid where t2.operatorid=v_operatorid and t1.systemid=v_systemid and t1.isused=1 and t2.isused=1) t;
		select array_to_json(array_agg(row_to_json(t))) into v_return from (select t2.operatorid,t3.systemid,t2.operatorname,t2.sex,t2.phone,t2.memo,t2.mycode,t2.upcode,t2.headimgurl,t2.nickname,t2.tokeninterval,t3.systemname,t3.algorithm,t3.prikey from sysinfo.operinfo t2,sysinfo.systeminfo t3 where t2.operatorid=v_operatorid and t3.systemid=v_systemid) t; 
		v_return:=v_return->0;
		--select orgid,orgname,orgtype,orgtypename from sysinfo.sysorg t1 left join 
		select params into v_keystr from sysinfo.actions where actionid=101;
		v_token:=case (v_return->>'algorithm')::integer
		  when 1 then gm.aes_encrypt(json_build_object('systemid',v_return->'systemid','token',gm.aes_encrypt(json_build_object('operatorno',v_return->'operatorid','tokentime',now()::varchar,'tokeninterval',v_return->>'tokeninterval')::text, (v_return->>'prikey')::text))::text,v_keystr)
			when 2 then gm.aes_encrypt(json_build_object('systemid',v_return->'systemid','token',gm.aes_encrypt(json_build_object('operatorno',v_return->'operatorid','systemid',v_return->'systemid','tokentime',now()::varchar,'tokeninterval',v_return->>'tokeninterval')::text, (v_return->>'prikey')::text))::text,v_keystr)
--		  when 3 then encode(private.hmac(json_build_object('operatorno',v_return->'operatorid','systemid',v_return->'systemid','orgid',v_return->'orgid','tokentime',now()::varchar)::text,v_return->>'prikey','sha512'),'hex')		
--			else null
		end;

  update sysinfo.operinfo set tokentime = now()::timestamp + (interval '1 minute')*tokeninterval where operatorid = (v_return->>'operatorid')::integer;
select json_build_object('token',v_token,'operator',json_build_object('sex',v_return->>'sex','memo',v_return->>'memo','org',v_return->>'org','mycode',v_return->>'mycode','upcode',v_return->>'upcode','birthday',v_return->>'birthday','headimgurl',v_return->>'headimgurl','operatorname',v_return->>'operatorname','systemname',v_return->>'systemname','org',v_orgjson),'actions',array_to_json(array_agg(actionid))) into v_return from (
select distinct coalesce(t1.sysactionid,t1.actionid) actionid from sysinfo.sysaction t1,(
select distinct '.'||idpath||'.' actioncode from (
with recursive c as (
	select c3.sysactionid,c3.permissiontype from sysinfo.operpermission c2 left join sysinfo.rolepermission c3 on c2.sysactionid=c3.roleinfoid where c2.permissiontype=2 and c2.operatorid=(v_return->>'operatorid')::integer and c2.systemid=(v_return->>'systemid')::integer
	union 
	select c3.sysactionid,c3.permissiontype from c left join sysinfo.rolepermission c3 on c.sysactionid=c3.roleinfoid where c.permissiontype=2
	) select sysactionid from c
  union
	select t1.sysactionid from sysinfo.operpermission t1 where t1.operatorid=(v_return->>'operatorid')::integer and t1.permissiontype=1 and t1.systemid=(v_return->>'systemid')::integer
) t1
--select t2.actionid from sysinfo.operpermission t1 left join sysinfo.rolepermission t2 on t1.actionid=t2.roleinfoid and t2.systemid=(v_return->>'systemid')::integer where operatorid=(v_return->>'operatorid')::integer and t2.permissiontype=2 and t1.systemid=(v_return->>'systemid')::integer ) t1
left join sysinfo.sysaction t2 on t1.sysactionid=t2.sysactionid where  systemid=(v_return->>'systemid')::integer) t2
where position('.'||t1.idpath||'.' in t2.actioncode)>0 
   or position(t2.actioncode in '.'||t1.idpath||'.')>0
	 order by actionid) t;
	 
		
	
	return gm.returnjson(0,v_return);
END$$;


ALTER FUNCTION sysinfo.loginaccount(p_loginname character varying, p_system character varying, p_ip character varying, p_accounttype integer, p_appid integer) OWNER TO gm;

--
-- Name: operinfo_add(character varying, bigint, character varying, character varying, smallint, integer, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfo_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;v_pass varchar;v_mycode int;
begin
  v_check:=gm.check_login(p_token,126);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo where accounts=p_accounts and operatorid<>p_operatorid;
	if v_c>0 then return gm.returnjson(100022);end if;--账号不能重复！
	if p_tokentype<>1 then p_tokentype:=2;end if;
  if p_operatorid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.operinfo_operatorid_seq');
      select count(*) into v_c from sysinfo.operinfo where operatorid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.operinfo where operatorid=p_operatorid;
    if v_c>0 then return gm.returnjson(100011); end if;--操作员信息出错！
    v_id:=p_operatorid;
  end if;
	v_pass:=substring(md5(random()::varchar),2,8);
	if p_tokentype!=1 then p_tokentype:=2;end if;
  insert into sysinfo.operinfo( operatorid,operatorname,accounts,pass,isused,tokentype,tokeninterval,createoperator,createtime) values(v_id,p_operatorname,p_accounts,v_pass,1,p_tokentype,p_tokeninterval,(v_check->'info'->>'operatorid')::integer,now() );
  insert into sysinfo.operpermission( operatorid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct v_id,nextval('sysinfo.operpermission_permissionid_seq'),permissiontype,ifpermission,permissionorder,params,sysactionid  from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) t  where  (permissiontype=1 and sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (permissiontype=2 and sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer));
	delete from sysinfo.sysoper where operatorid=v_id ;
  insert into sysinfo.sysoper( systemid,operatorid) select distinct systemid,v_id from (
	select systemid from sysinfo.roleinfo where roleinfoid in (select sysactionid from sysinfo.operpermission where permissiontype=2 and operatorid=v_id)
    union 
  select systemid from sysinfo.sysaction where sysactionid in (select sysactionid from sysinfo.operpermission where permissiontype=1 and operatorid=v_id) ) t;
  return gm.returnjson(0,json_build_object('id',v_id,'pass',v_pass));
end; 
$$;


ALTER FUNCTION sysinfo.operinfo_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfo_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfo_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) IS '新增员工及权限(全系统权限)
operatorid:员工编号
operatorname:员工姓名
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
operpermissionjson:员工权限{"permissiontype":权限类型（1=员工直接权限，sysactionid 存 sysaction.sysactionid；2=角色权限，sysactionid 存 roleinfo.roleinfoid）,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,"permissionid":操作员权限编号}
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id,"pass":密码}'';';


--
-- Name: operinfo_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfo_del(p_token character varying, p_operatorid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,126);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  -- 系统内置操作员（10000/100000）禁止删除：删除后系统将无法登录，需重新安装
  if p_operatorid in (10000,100000) then return gm.returnjson(100036); end if;
  select count(*) into v_c from sysinfo.operinfo where operatorid=p_operatorid;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  delete from sysinfo.sysoper where operatorid=p_operatorid;
  update sysinfo.operinfo set isused=0,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where operatorid=p_operatorid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.operinfo_del(p_token character varying, p_operatorid integer) OWNER TO gm;

--
-- Name: FUNCTION operinfo_del(p_token character varying, p_operatorid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfo_del(p_token character varying, p_operatorid integer) IS '删除员工
operatorid:id';


--
-- Name: operinfo_edit(character varying, bigint, character varying, character varying, smallint, integer, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfo_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,126);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo where accounts=p_accounts and operatorid<>p_operatorid;
	if v_c>0 then return gm.returnjson(100022);end if;--账号不能重复！	
  select count(*) into v_c from sysinfo.operinfo where operatorid=p_operatorid and isused=1;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
	if p_tokentype<>1 then p_tokentype:=2;end if;
  -- 子表按 permissionid 增删改：先按提交主键差集删除、再按主键更新、最后插入新行
  -- （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）
  delete from sysinfo.operpermission t where t.operatorid=p_operatorid and not exists (select 1 from (select (elem->>'permissionid')::int4 as permissionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where s.permissionid=t.permissionid);
  update sysinfo.operinfo set operatorname=p_operatorname,accounts=p_accounts,tokentype=p_tokentype,tokeninterval=p_tokeninterval,updateoperator=(v_check->'info'->>'operatorid')::integer,updatetime=now() where operatorid=p_operatorid;
	delete from sysinfo.sysoper where operatorid=p_operatorid ;
  insert into sysinfo.sysoper( systemid,operatorid) select distinct systemid,p_operatorid from (
	select systemid from sysinfo.roleinfo where roleinfoid in (select sysactionid from sysinfo.operpermission where permissiontype=2 and operatorid=p_operatorid)
    union 
  select systemid from sysinfo.sysaction where sysactionid in (select sysactionid from sysinfo.operpermission where permissiontype=1 and operatorid=p_operatorid) ) t;
  update sysinfo.operpermission t set permissiontype=s.permissiontype,ifpermission=s.ifpermission,permissionorder=s.permissionorder,params=s.params,sysactionid=s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where t.permissionid=s.permissionid and t.operatorid=p_operatorid and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  insert into sysinfo.operpermission( operatorid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct p_operatorid,nextval('sysinfo.operpermission_permissionid_seq'),s.permissiontype,s.ifpermission,s.permissionorder,s.params,s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where (s.permissionid is null or not exists (select 1 from sysinfo.operpermission t2 where t2.permissionid=s.permissionid)) and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.operinfo_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfo_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfo_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) IS '修改员工及权限(全系统权限)
operatorid:员工编号
operatorname:员工姓名
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
operpermissionjson:员工权限{"permissiontype":权限类型（1=员工直接权限，sysactionid 存 sysaction.sysactionid；2=角色权限，sysactionid 存 roleinfo.roleinfoid）,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,"permissionid":操作员权限编号}
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id,"pass":密码}'';';


--
-- Name: operinfo_merge(character varying, bigint, character varying, character varying, smallint, integer, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfo_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,126);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo where accounts=p_accounts and operatorid<>p_operatorid;
	if v_c>0 then return gm.returnjson(100022);end if;--账号不能重复！	
  if p_operatorid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.operinfo_operatorid_seq');
      select count(*) into v_c from sysinfo.operinfo where operatorid=v_id;
    end loop;
  else
    v_id:=p_operatorid;
  end if;
	if p_tokentype<>1 then p_tokentype:=2;end if;
  -- 子表按 permissionid 增删改：先按提交主键差集删除、再按主键更新、最后插入新行
  -- （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）
  delete from sysinfo.operpermission t where t.operatorid=v_id and not exists (select 1 from (select (elem->>'permissionid')::int4 as permissionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where s.permissionid=t.permissionid);
  with "te" as (update sysinfo.operinfo set operatorname=p_operatorname,accounts=p_accounts,tokentype=p_tokentype,tokeninterval=p_tokeninterval,updateoperator=(v_check->'info'->>'operatorid')::integer,updatetime=now() where operatorid=v_id returning *)
  insert into sysinfo.operinfo ( operatorid,operatorname,accounts,isused,tokentype,tokeninterval,createoperator,createtime) select v_id,p_operatorname,p_accounts,1,p_tokentype,p_tokeninterval,(v_check->'info'->>'operatorid')::integer,now() where (select count(*) from te) = 0;
	delete from sysinfo.sysoper where operatorid=v_id ;
  insert into sysinfo.sysoper( systemid,operatorid) select distinct systemid,v_id from (
	select systemid from sysinfo.roleinfo where roleinfoid in (select sysactionid from sysinfo.operpermission where permissiontype=2 and operatorid=v_id)
    union 
  select systemid from sysinfo.sysaction where sysactionid in (select sysactionid from sysinfo.operpermission where permissiontype=1 and operatorid=v_id) ) t;
  update sysinfo.operpermission t set permissiontype=s.permissiontype,ifpermission=s.ifpermission,permissionorder=s.permissionorder,params=s.params,sysactionid=s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where t.permissionid=s.permissionid and t.operatorid=v_id and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  insert into sysinfo.operpermission( operatorid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct v_id,nextval('sysinfo.operpermission_permissionid_seq'),s.permissiontype,s.ifpermission,s.permissionorder,s.params,s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where (s.permissionid is null or not exists (select 1 from sysinfo.operpermission t2 where t2.permissionid=s.permissionid)) and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  return gm.returnjson(0,json_build_object('id',v_id));
end;
$$;


ALTER FUNCTION sysinfo.operinfo_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfo_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfo_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_accounts character varying, p_tokentype smallint, p_tokeninterval integer, p_operpermissionjson json) IS '合并员工及权限(全系统权限)
operatorid:员工编号
operatorname:员工姓名
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
operpermissionjson:员工权限{"permissiontype":权限类型（1=员工直接权限，sysactionid 存 sysaction.sysactionid；2=角色权限，sysactionid 存 roleinfo.roleinfoid）,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,"permissionid":操作员权限编号}
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id,"pass":密码}'';';


--
-- Name: operinfo_query(character varying, integer, integer, bigint, character varying, smallint, character varying, character varying, character varying, integer, character varying, character varying, text, character varying, timestamp without time zone, timestamp without time zone, smallint, integer); Type: FUNCTION; Schema: sysinfo; Owner: postgres
--

CREATE FUNCTION sysinfo.operinfo_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_isused integer, p_mycode character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,126);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.operatorid,t1.operatorname,t1.sex,t1.phone,t1.accounts,t1.memo,t1.isused,t1.mycode,t1.upcode,t1.headimgurl,t1.nickname,t1.birthday::varchar,t1.tokentype,t1.tokeninterval,sysoper.sysoperjson,operpermission.operpermissionjson,t1.createoperator,t2.operatorname createname,t1.createtime::varchar,t1.updateoperator ,t3.operatorname updatename,t1.updatetime::varchar,t1.deloperator,t4.operatorname delname,t1.deltime::varchar from sysinfo.operinfo t1 left join (select operatorid,array_to_json(array_agg(row_to_json(t))) sysoperjson from (select  t1.operatorid,t1.systemid,ts.systemname from sysinfo.sysoper t1 left join sysinfo.systeminfo ts on t1.systemid=ts.systemid )t group by t.operatorid) sysoper on t1.operatorid=sysoper.operatorid left join (select operatorid,array_to_json(array_agg(row_to_json(t))) operpermissionjson from (select t1.operatorid,t1.permissionid,t1.permissiontype,t1.ifpermission,t1.permissionorder,t1.params,t1.sysactionid,case t1.permissiontype when 1 then t2.sysactionname else t3.operatorname end sysactionname from sysinfo.operpermission t1 left join sysinfo.sysaction t2 on t2.sysactionid=t1.sysactionid left join sysinfo.operinfo t3 on t3.operatorid=t1.sysactionid) t group by t.operatorid) operpermission on t1.operatorid=operpermission.operatorid left join sysinfo.operinfo t2 on t1.createoperator=t2.operatorid left join sysinfo.operinfo t3 on t1.updateoperator=t3.operatorid left join sysinfo.operinfo t4 on t1.deloperator=t4.operatorid where (p_operatorid is null or p_operatorid=t1.operatorid) and (p_operatorname is null or position(p_operatorname in t1.operatorname )>0) and (p_sex is null or p_sex=t1.sex) and (p_phone is null or position(p_phone in t1.phone )>0) and (p_accounts is null or position(p_accounts in t1.accounts )>0) and (p_memo is null or position(p_memo in t1.memo )>0) and (p_isused is null or p_isused=t1.isused) and (p_mycode is null or p_mycode=t1.mycode) and (p_upcode is null or p_upcode=t1.upcode) and (p_headimgurl is null or position(p_headimgurl in t1.headimgurl )>0) and (p_nickname is null or position(p_nickname in t1.nickname )>0) and (p_beginbirthday is null or t1.birthday>=p_beginbirthday) and (p_endbirthday is null or t1.birthday<=p_endbirthday) and (p_tokentype is null or p_tokentype=t1.tokentype) and (p_tokeninterval is null or p_tokeninterval=t1.tokeninterval) order by t1.operatorid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.operinfo t1 where  (p_operatorid is null or p_operatorid=operatorid) and (p_operatorname is null or position(p_operatorname in t1.operatorname )>0) and (p_sex is null or p_sex=t1.sex) and (p_phone is null or position(p_phone in t1.phone )>0) and (p_accounts is null or position(p_accounts in t1.accounts )>0) and (p_memo is null or position(p_memo in t1.memo )>0) and (p_isused is null or p_isused=t1.isused) and (p_mycode is null or p_mycode=t1.mycode) and (p_upcode is null or p_upcode=t1.upcode) and (p_headimgurl is null or position(p_headimgurl in t1.headimgurl )>0) and (p_nickname is null or position(p_nickname in t1.nickname )>0) and (p_beginbirthday is null or t1.birthday>=p_beginbirthday) and (p_endbirthday is null or t1.birthday<=p_endbirthday) and (p_tokentype is null or p_tokentype=t1.tokentype) and (p_tokeninterval is null or p_tokeninterval=t1.tokeninterval);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.operatorid,t1.operatorname,t1.sex,t1.phone,t1.accounts,t1.memo,t1.isused,t1.mycode,t1.upcode,t1.headimgurl,t1.nickname,t1.birthday::varchar,t1.tokentype,t1.tokeninterval,sysoper.sysoperjson,operpermission.operpermissionjson,t1.createoperator,t2.operatorname createname,t1.createtime::varchar,t1.updateoperator ,t3.operatorname updatename,t1.updatetime::varchar,t1.deloperator,t4.operatorname delname,t1.deltime::varchar from sysinfo.operinfo t1 left join (select operatorid,array_to_json(array_agg(row_to_json(t))) sysoperjson from (select  t1.operatorid,t1.systemid,ts.systemname from sysinfo.sysoper t1 left join sysinfo.systeminfo ts on t1.systemid=ts.systemid )t group by t.operatorid) sysoper on t1.operatorid=sysoper.operatorid left join (select operatorid,array_to_json(array_agg(row_to_json(t))) operpermissionjson from (select t1.operatorid,t1.permissionid,t1.permissiontype,t1.ifpermission,t1.permissionorder,t1.params,t1.sysactionid,case t1.permissiontype when 1 then t2.sysactionname else t3.operatorname end sysactionname from sysinfo.operpermission t1 left join sysinfo.sysaction t2 on t2.sysactionid=t1.sysactionid left join sysinfo.operinfo t3 on t3.operatorid=t1.sysactionid) t group by t.operatorid) operpermission on t1.operatorid=operpermission.operatorid left join sysinfo.operinfo t2 on t1.createoperator=t2.operatorid left join sysinfo.operinfo t3 on t1.updateoperator=t3.operatorid left join sysinfo.operinfo t4 on t1.deloperator=t4.operatorid where (p_operatorid is null or p_operatorid=t1.operatorid) and (p_operatorname is null or position(p_operatorname in t1.operatorname )>0) and (p_sex is null or p_sex=t1.sex) and (p_phone is null or position(p_phone in t1.phone )>0) and (p_accounts is null or position(p_accounts in t1.accounts )>0) and (p_memo is null or position(p_memo in t1.memo )>0) and (p_isused is null or p_isused=t1.isused) and (p_mycode is null or p_mycode=t1.mycode) and (p_upcode is null or p_upcode=t1.upcode) and (p_headimgurl is null or position(p_headimgurl in t1.headimgurl )>0) and (p_nickname is null or position(p_nickname in t1.nickname )>0) and (p_beginbirthday is null or t1.birthday>=p_beginbirthday) and (p_endbirthday is null or t1.birthday<=p_endbirthday) and (p_tokentype is null or p_tokentype=t1.tokentype) and (p_tokeninterval is null or p_tokeninterval=t1.tokeninterval) order by t1.operatorid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.operinfo_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_isused integer, p_mycode character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) OWNER TO postgres;

--
-- Name: operinfo_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfo_undel(p_token character varying, p_operatorid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,127);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo where operatorid=p_operatorid;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！

  update sysinfo.operinfo set isused=1,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where operatorid=p_operatorid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.operinfo_undel(p_token character varying, p_operatorid integer) OWNER TO gm;

--
-- Name: FUNCTION operinfo_undel(p_token character varying, p_operatorid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfo_undel(p_token character varying, p_operatorid integer) IS '恢复员工
operatorid:id';


--
-- Name: operinfoorg_add(character varying, bigint, character varying, smallint, character varying, character varying, character varying, character varying, text, character varying, timestamp without time zone, smallint, integer, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfoorg_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;v_pass varchar;v_mycode varchar;
begin
  v_check:=gm.check_login(p_token,144);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_operatorid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.operinfo_operatorid_seq');
      select count(*) into v_c from sysinfo.operinfo where operatorid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.operinfo where operatorid=p_operatorid;
    if v_c>0 then return gm.returnjson(100011); end if;--操作员信息出错！
    v_id:=p_operatorid;
  end if;
	v_pass:=substring(md5(random()::varchar|| clock_timestamp()),1,8);
	v_c:=1;loop exit when (v_c>0);
  	v_mycode:=substring(md5(random()::varchar|| clock_timestamp()),1,8);
	  select count(*) into v_c from sysinfo.operinfo where mycode=v_mycode;
	end loop;	
  insert into sysinfo.sysoperorg( operatorid,sysorgid) select distinct v_id,sysorgid  from (select (elem->>'operatorid')::int4 as operatorid,(elem->>'sysorgid')::int4 as sysorgid from jsonb_array_elements(p_sysoperorgjson::jsonb) as elem) t ;
	insert into sysinfo.sysoper values(v_id,(v_check->'info'->>'systemid')::integer);
  insert into sysinfo.operinfo( operatorid,operatorname,sex,phone,accounts,memo,isused,pass,mycode,upcode,headimgurl,nickname,birthday,tokentype,tokeninterval,createoperator,createtime) values(v_id,p_operatorname,p_sex,p_phone,p_accounts,p_memo,1,v_pass,v_mycode,p_upcode,p_headimgurl,p_nickname,p_birthday,p_tokentype,p_tokeninterval ,(v_check->'info'->>'operatorid')::integer,now());
  return gm.returnjson(0,json_build_object('id',v_id,'pass',v_pass));
end; 
$$;


ALTER FUNCTION sysinfo.operinfoorg_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfoorg_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfoorg_add(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) IS '新增员工部门
operatorid:员工编号
operatorname:员工姓名
sex:性别
phone:电话
accounts:帐号
pass:密码
tokenkey:令牌
tokentime:令牌时间
memo:备注
mycode:推广码
upcode:推广上级id
headimgurl:头像url
nickname:昵称
birthday:生日
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
sysoperorgjson:操作员部门{"sysorgid":系统部门id,
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: operinfoorg_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfoorg_del(p_token character varying, p_operatorid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,144);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  -- 系统内置操作员（10000/100000）禁止删除
  if p_operatorid in (10000,100000) then return gm.returnjson(100036); end if;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t2.systemid=(v_check->'info'->>'systemid')::integer and isused=1;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  delete from sysinfo.sysoperorg where operatorid=p_operatorid and sysorgid in (select sysorgid from sysinfo.sysorg where systemid=(v_check->'info'->>'systemid')::integer);
  update sysinfo.operinfo set isused=0,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where operatorid=p_operatorid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.operinfoorg_del(p_token character varying, p_operatorid integer) OWNER TO gm;

--
-- Name: FUNCTION operinfoorg_del(p_token character varying, p_operatorid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfoorg_del(p_token character varying, p_operatorid integer) IS '删除员工部门
operatorid:id';


--
-- Name: operinfoorg_edit(character varying, bigint, character varying, smallint, character varying, character varying, character varying, character varying, text, character varying, timestamp without time zone, smallint, integer, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfoorg_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,144);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  v_id:=p_operatorid;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t2.systemid=(v_check->'info'->>'systemid')::integer and isused=1;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  delete from sysinfo.sysoperorg where operatorid=p_operatorid and sysorgid in (select sysorgid from sysinfo.sysorg where systemid=(v_check->'info'->>'systemid')::integer);
  insert into sysinfo.sysoperorg( operatorid,sysorgid) select distinct v_id,sysorgid  from json_to_recordset(p_sysoperorgjson::json) as t(operatorid int4,sysorgid int4) where sysorgid in (select sysorgid from sysinfo.sysorg where systemid=(v_check->'info'->>'systemid')::integer);
  update sysinfo.operinfo set operatorname=p_operatorname,sex=p_sex,phone=p_phone,accounts=p_accounts,memo=p_memo,upcode=p_upcode,headimgurl=p_headimgurl,nickname=p_nickname,birthday=p_birthday,tokentype=p_tokentype,tokeninterval=p_tokeninterval ,updateoperator=(v_check->'info'->>'operatorid')::integer,updatetime=now() where operatorid=p_operatorid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.operinfoorg_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfoorg_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfoorg_edit(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) IS '修改员工部门
operatorid:员工编号
operatorname:员工姓名
sex:性别
phone:电话
accounts:帐号
pass:密码
tokenkey:令牌
tokentime:令牌时间
memo:备注
mycode:推广码
upcode:推广上级id
headimgurl:头像url
nickname:昵称
birthday:生日
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
sysoperorgjson:操作员部门{"sysorgid":系统部门id,
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: operinfoorg_merge(character varying, bigint, character varying, smallint, character varying, character varying, character varying, character varying, text, character varying, timestamp without time zone, smallint, integer, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfoorg_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;v_pass varchar;v_mycode varchar;
begin
  v_check:=gm.check_login(p_token,144);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_operatorid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.operinfo_operatorid_seq');
      select count(*) into v_c from sysinfo.operinfo where operatorid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t2.systemid=(v_check->'info'->>'systemid')::integer and isused=1;
    if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！	
    v_id:=p_operatorid;
  end if;
	v_pass:=substring(md5(random()::varchar|| clock_timestamp()),1,8);
	v_c:=1;loop exit when (v_c>0);
    v_mycode:=substring(md5(random()::varchar|| clock_timestamp()),1,8);
	  select count(*) into v_c from sysinfo.operinfo where mycode=v_mycode;
	end loop;	
  delete from sysinfo.sysoperorg where operatorid=v_id and sysorgid in (select sysorgid from sysinfo.sysorg where systemid=(v_check->'info'->>'systemid')::integer);
  insert into sysinfo.sysoperorg( operatorid,sysorgid) select distinct v_id,sysorgid  from (select (elem->>'operatorid')::int4 as operatorid,(elem->>'sysorgid')::int4 as sysorgid from jsonb_array_elements(p_sysoperorgjson::jsonb) as elem) t where sysorgid in (select sysorgid from sysinfo.sysorg where systemid=(v_check->'info'->>'systemid')::integer);
  with "te" as (update sysinfo.operinfo set operatorname=p_operatorname,sex=p_sex,phone=p_phone,accounts=p_accounts,memo=p_memo,upcode=p_upcode,headimgurl=p_headimgurl,nickname=p_nickname,birthday=p_birthday,tokentype=p_tokentype,tokeninterval=p_tokeninterval ,updateoperator=(v_check->'info'->>'operatorid')::integer,updatetime=now() where operatorid=v_id returning *)
  insert into sysinfo.operinfo ( operatorid,operatorname,sex,phone,accounts,pass,memo,isused,mycode,upcode,headimgurl,nickname,birthday,tokentype,tokeninterval,createoperator,createtime) select v_id,p_operatorname,p_sex,p_phone,p_accounts,v_pass,p_memo,1,v_mycode,p_upcode,p_headimgurl,p_nickname,p_birthday,p_tokentype,p_tokeninterval ,(v_check->'info'->>'operatorid')::integer,now() where (select count(*) from te) = 0;
  insert into sysinfo.sysoper( systemid,operatorid) select (v_check->'info'->>'systemid')::integer, v_id where not exists (select 1 from sysinfo.sysoper where operatorid=v_id and systemid=(v_check->'info'->>'systemid')::integer);
  return gm.returnjson(0,json_build_object('id',v_id,'pass',v_pass));
end; 
$$;


ALTER FUNCTION sysinfo.operinfoorg_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfoorg_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfoorg_merge(p_token character varying, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_birthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer, p_sysoperorgjson json) IS '合并员工部门
operatorid:员工编号
operatorname:员工姓名
sex:性别
phone:电话
accounts:帐号
pass:密码
tokenkey:令牌
tokentime:令牌时间
memo:备注
mycode:推广码
upcode:推广上级id
headimgurl:头像url
nickname:昵称
birthday:生日
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
sysoperorgjson:操作员部门{"sysorgid":系统部门id,
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: operinfoorg_query(character varying, integer, integer, bigint, character varying, smallint, character varying, character varying, character varying, integer, character varying, character varying, text, character varying, timestamp without time zone, timestamp without time zone, smallint, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfoorg_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_isused integer, p_mycode character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,144);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.operatorid,t1.operatorname,t1.sex,t1.phone,t1.accounts,t1.memo,t1.isused,t1.mycode,t1.upcode,t1.headimgurl,t1.nickname,t1.birthday::varchar,t1.tokentype,t1.tokeninterval,t1.createoperator,tcreateoperator.operatorname createoperatorname,t1.createtime::varchar,t1.updateoperator,tupdateoperator.operatorname updateoperatorname,t1.updatetime::varchar,t1.deloperator,tdeloperator.operatorname deloperatorname,t1.deltime::varchar,sysoperorg.sysoperorgjson from sysinfo.operinfo t1 left join sysinfo.operinfo tcreateoperator on t1.createoperator=tcreateoperator.operatorid left join sysinfo.operinfo tupdateoperator on t1.updateoperator=tupdateoperator.operatorid left join sysinfo.operinfo tdeloperator on t1.deloperator=tdeloperator.operatorid left join (select operatorid,array_to_json(array_agg(row_to_json(t))) sysoperorgjson from (select  t1.operatorid,t1.sysorgid,t2.sysorgname from sysinfo.sysoperorg t1 left join sysinfo.sysorg t2 on t2.sysorgid=t1.sysorgid where systemid=(v_check->'info'->>'systemid')::integer order by t1.operatorid ) t group by t.operatorid) sysoperorg on t1.operatorid=sysoperorg.operatorid left join sysinfo.sysoper ts on ts.operatorid=t1.operatorid where  (p_operatorid is null or p_operatorid=t1.operatorid) and (p_operatorname is null or position(p_operatorname in t1.operatorname )>0) and (p_sex is null or p_sex=t1.sex) and (p_phone is null or position(p_phone in t1.phone )>0) and (p_accounts is null or position(p_accounts in t1.accounts )>0) and (p_memo is null or position(p_memo in t1.memo )>0) and (p_isused is null or p_isused=t1.isused) and (p_mycode is null or position(p_mycode in t1.mycode )>0) and (p_upcode is null or position(p_upcode in t1.upcode )>0) and (p_headimgurl is null or position(p_headimgurl in t1.headimgurl )>0) and (p_nickname is null or position(p_nickname in t1.nickname )>0) and (p_beginbirthday is null or t1.birthday>=p_beginbirthday) and (p_endbirthday is null or t1.birthday<=p_endbirthday) and (p_tokentype is null or p_tokentype=t1.tokentype) and (p_tokeninterval is null or p_tokeninterval=t1.tokeninterval) and systemid=(v_check->'info'->>'systemid')::integer order by t1.operatorid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper ts on ts.operatorid=t1.operatorid where  (p_operatorid is null or p_operatorid=t1.operatorid) and (p_operatorname is null or position(p_operatorname in t1.operatorname )>0) and (p_sex is null or p_sex=t1.sex) and (p_phone is null or position(p_phone in t1.phone )>0) and (p_accounts is null or position(p_accounts in t1.accounts )>0) and (p_memo is null or position(p_memo in t1.memo )>0) and (p_isused is null or p_isused=t1.isused) and (p_mycode is null or position(p_mycode in t1.mycode )>0) and (p_upcode is null or position(p_upcode in t1.upcode )>0) and (p_headimgurl is null or position(p_headimgurl in t1.headimgurl )>0) and (p_nickname is null or position(p_nickname in t1.nickname )>0) and (p_beginbirthday is null or t1.birthday>=p_beginbirthday) and (p_endbirthday is null or t1.birthday<=p_endbirthday) and (p_tokentype is null or p_tokentype=t1.tokentype) and (p_tokeninterval is null or p_tokeninterval=t1.tokeninterval) and ts.systemid=(v_check->'info'->>'systemid')::integer;
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.operatorid,t1.operatorname,t1.sex,t1.phone,t1.accounts,t1.memo,t1.isused,t1.mycode,t1.upcode,t1.headimgurl,t1.nickname,t1.birthday::varchar,t1.tokentype,t1.tokeninterval,t1.createoperator,tcreateoperator.operatorname createoperatorname,t1.createtime::varchar,t1.updateoperator,tupdateoperator.operatorname updateoperatorname,t1.updatetime::varchar,t1.deloperator,tdeloperator.operatorname deloperatorname,t1.deltime::varchar,sysoperorg.sysoperorgjson from sysinfo.operinfo t1 left join sysinfo.operinfo tcreateoperator on t1.createoperator=tcreateoperator.operatorid left join sysinfo.operinfo tupdateoperator on t1.updateoperator=tupdateoperator.operatorid left join sysinfo.operinfo tdeloperator on t1.deloperator=tdeloperator.operatorid left join (select operatorid,array_to_json(array_agg(row_to_json(t))) sysoperorgjson from (select  t1.operatorid,t1.sysorgid,t2.sysorgname from sysinfo.sysoperorg t1 left join sysinfo.sysorg t2 on t2.sysorgid=t1.sysorgid) t group by t.operatorid) sysoperorg on t1.operatorid=sysoperorg.operatorid left join sysinfo.sysoper ts on ts.operatorid=t1.operatorid where  (p_operatorid is null or p_operatorid=t1.operatorid) and (p_operatorname is null or position(p_operatorname in t1.operatorname )>0) and (p_sex is null or p_sex=t1.sex) and (p_phone is null or position(p_phone in t1.phone )>0) and (p_accounts is null or position(p_accounts in t1.accounts )>0) and (p_memo is null or position(p_memo in t1.memo )>0) and (p_isused is null or p_isused=t1.isused) and (p_mycode is null or position(p_mycode in t1.mycode )>0) and (p_upcode is null or position(p_upcode in t1.upcode )>0) and (p_headimgurl is null or position(p_headimgurl in t1.headimgurl )>0) and (p_nickname is null or position(p_nickname in t1.nickname )>0) and (p_beginbirthday is null or t1.birthday>=p_beginbirthday) and (p_endbirthday is null or t1.birthday<=p_endbirthday) and (p_tokentype is null or p_tokentype=t1.tokentype) and (p_tokeninterval is null or p_tokeninterval=t1.tokeninterval) and ts.systemid=(v_check->'info'->>'systemid')::integer order by t1.operatorid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.operinfoorg_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_isused integer, p_mycode character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) OWNER TO gm;

--
-- Name: FUNCTION operinfoorg_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_isused integer, p_mycode character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfoorg_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_isused integer, p_mycode character varying, p_upcode character varying, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) IS '查询员工部门
operatorid:员工编号
operatorname:员工姓名
sex:性别
phone:电话
accounts:帐号
pass:密码
tokenkey:令牌
begintokentime:令牌时间
endtokentime:令牌时间结束
memo:备注
isused:是否使用
mycode:推广码
upcode:推广上级id
headimgurl:头像url
nickname:昵称
beginbirthday:生日
endbirthday:生日结束
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
返回:{"total":总记录数,"rows":[{"operatorid":操作员id,"operatorname":操作员姓名,"operatorname":员工姓名,"sex":性别,"phone":电话,"accounts":帐号,"pass":密码,"tokenkey":令牌,"tokentime":令牌时间,"memo":备注,"isused":使用,"mycode":推广码,"upcode":推广上级id,"headimgurl":头像url,"nickname":昵称,"birthday":生日,"tokentype":令牌类型1默认单人登录2多人登录,"tokeninterval":令牌时长默认180分钟,"createoperator":操作员编号,"createoperatorname":创建人员,"updateoperator":操作员编号,"updateoperatorname":修改人员,"deloperator":操作员编号,"deloperatorname":删除人员}]}';


--
-- Name: operinfoorg_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfoorg_undel(p_token character varying, p_operatorid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,145);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t2.systemid=(v_check->'info'->>'systemid')::integer and isused=0;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！

  update sysinfo.operinfo set isused=1,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where operatorid=p_operatorid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.operinfoorg_undel(p_token character varying, p_operatorid integer) OWNER TO gm;

--
-- Name: FUNCTION operinfoorg_undel(p_token character varying, p_operatorid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfoorg_undel(p_token character varying, p_operatorid integer) IS '恢复员工部门
operatorid:id';


--
-- Name: operinfopermission_add(character varying, bigint, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfopermission_add(p_token character varying, p_operatorid bigint, p_operpermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,138);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and isused=1 and systemid=(v_check->'info'->>'systemid')::integer;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  insert into sysinfo.operpermission( operatorid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct p_operatorid,nextval('sysinfo.operpermission_permissionid_seq'),permissiontype,ifpermission,permissionorder,params,sysactionid  from json_to_recordset(p_operpermissionjson::json) as t(permissionid int4,permissiontype int2,ifpermission int2,permissionorder int4,params varchar,sysactionid int4)  where  (permissiontype=1 and sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (permissiontype=2 and sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer));
  return gm.returnjson(0,json_build_object('id',p_operatorid));
end; 
$$;


ALTER FUNCTION sysinfo.operinfopermission_add(p_token character varying, p_operatorid bigint, p_operpermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfopermission_add(p_token character varying, p_operatorid bigint, p_operpermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfopermission_add(p_token character varying, p_operatorid bigint, p_operpermissionjson json) IS '新增员工权限(本系统权限)
operatorid:员工编号
operpermissionjson:员工权限{"permissiontype":权限类型（1=员工直接权限，sysactionid 存 sysaction.sysactionid；2=角色权限，sysactionid 存 roleinfo.roleinfoid）,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,"permissionid":操作员权限编号}
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: operinfopermission_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfopermission_del(p_token character varying, p_operatorid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,141);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  -- 系统内置操作员（10000/100000）禁止删除
  if p_operatorid in (10000,100000) then return gm.returnjson(100036); end if;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t1.isused=1 and t2.systemid=(v_check->'info'->>'systemid')::integer;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  delete from sysinfo.operpermission where operatorid=p_operatorid and ((permissiontype=1 and sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (permissiontype=2 and sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer) ));
	delete from sysinfo.sysoper where operatorid=p_operatorid;
	update sysinfo.operinfo set isused=0 where operatorid=p_operatorid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.operinfopermission_del(p_token character varying, p_operatorid integer) OWNER TO gm;

--
-- Name: FUNCTION operinfopermission_del(p_token character varying, p_operatorid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfopermission_del(p_token character varying, p_operatorid integer) IS '删除员工权限
operatorid:id';


--
-- Name: operinfopermission_edit(character varying, bigint, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfopermission_edit(p_token character varying, p_operatorid bigint, p_operpermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,139);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t1.isused=1 and t2.systemid=(v_check->'info'->>'systemid')::integer;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  -- 子表按 permissionid 增删改：先按提交主键差集删除、再按主键更新、最后插入新行
  -- （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）
  delete from sysinfo.operpermission t where t.operatorid=p_operatorid and ((t.permissiontype=1 and t.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (t.permissiontype=2 and t.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer))) and not exists (select 1 from (select (elem->>'permissionid')::int4 as permissionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where s.permissionid=t.permissionid);
  update sysinfo.operpermission t set permissiontype=s.permissiontype,ifpermission=s.ifpermission,permissionorder=s.permissionorder,params=s.params,sysactionid=s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where t.permissionid=s.permissionid and t.operatorid=p_operatorid and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  insert into sysinfo.operpermission( operatorid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct p_operatorid,nextval('sysinfo.operpermission_permissionid_seq'),s.permissiontype,s.ifpermission,s.permissionorder,s.params,s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where (s.permissionid is null or not exists (select 1 from sysinfo.operpermission t2 where t2.permissionid=s.permissionid)) and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.operinfopermission_edit(p_token character varying, p_operatorid bigint, p_operpermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfopermission_edit(p_token character varying, p_operatorid bigint, p_operpermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfopermission_edit(p_token character varying, p_operatorid bigint, p_operpermissionjson json) IS '修改员工权限(本系统权限)
operatorid:员工编号
operpermissionjson:员工权限{"permissiontype":权限类型（1=员工直接权限，sysactionid 存 sysaction.sysactionid；2=角色权限，sysactionid 存 roleinfo.roleinfoid）,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,"permissionid":操作员权限编号}
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: operinfopermission_merge(character varying, bigint, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfopermission_merge(p_token character varying, p_operatorid bigint, p_operpermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,140);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo t1 left join sysinfo.sysoper t2 on t1.operatorid=t2.operatorid where t1.operatorid=p_operatorid and t1.isused=1 and t2.systemid=(v_check->'info'->>'systemid')::integer;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！
  -- 子表按 permissionid 增删改：先按提交主键差集删除、再按主键更新、最后插入新行
  -- （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）
  delete from sysinfo.operpermission t where t.operatorid=p_operatorid and ((t.permissiontype=1 and t.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (t.permissiontype=2 and t.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer))) and not exists (select 1 from (select (elem->>'permissionid')::int4 as permissionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where s.permissionid=t.permissionid);
  update sysinfo.operpermission t set permissiontype=s.permissiontype,ifpermission=s.ifpermission,permissionorder=s.permissionorder,params=s.params,sysactionid=s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where t.permissionid=s.permissionid and t.operatorid=p_operatorid and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  insert into sysinfo.operpermission( operatorid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct p_operatorid,nextval('sysinfo.operpermission_permissionid_seq'),s.permissiontype,s.ifpermission,s.permissionorder,s.params,s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_operpermissionjson::jsonb) as elem) s where (s.permissionid is null or not exists (select 1 from sysinfo.operpermission t2 where t2.permissionid=s.permissionid)) and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  return gm.returnjson(0,json_build_object('id',p_operatorid));
end;
$$;


ALTER FUNCTION sysinfo.operinfopermission_merge(p_token character varying, p_operatorid bigint, p_operpermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION operinfopermission_merge(p_token character varying, p_operatorid bigint, p_operpermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfopermission_merge(p_token character varying, p_operatorid bigint, p_operpermissionjson json) IS '合并员工权限
operatorid:员工编号
operpermissionjson:员工权限{"permissiontype":权限类型（1=员工直接权限，sysactionid 存 sysaction.sysactionid；2=角色权限，sysactionid 存 roleinfo.roleinfoid）,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,"permissionid":操作员权限编号,"systemid":系统id,}
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: operinfopermission_query(character varying, integer, integer, bigint, character varying, smallint, character varying, character varying, character varying, character varying, timestamp without time zone, timestamp without time zone, character varying, integer, integer, integer, text, character varying, timestamp without time zone, timestamp without time zone, smallint, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.operinfopermission_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_pass character varying, p_tokenkey character varying, p_begintokentime timestamp without time zone, p_endtokentime timestamp without time zone, p_memo character varying, p_isused integer, p_mycode integer, p_upcode integer, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,143);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.operatorid,t1.operatorname,t1.isused,operpermission.operpermissionjson from sysinfo.operinfo t1 left join (select operatorid,array_to_json(array_agg(row_to_json(t))) operpermissionjson from (select  t1.operatorid,t1.permissiontype,t1.ifpermission,t1.permissionorder,t1.params,t1.sysactionid,t1.permissionid from sysinfo.operpermission t1)   t group by t.operatorid) operpermission on t1.operatorid=operpermission.operatorid where  t1.operatorid in (select operatorid from sysinfo.sysoper where systemid=(v_check->'info'->>'systemid')::integer) and (p_operatorid is null or p_operatorid=t1.operatorid)  order by t1.operatorid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.operinfo t1 where t1.operatorid in (select operatorid from sysinfo.sysoper where systemid=(v_check->'info'->>'systemid')::integer) and (p_operatorid is null or p_operatorid=operatorid) ;
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.operatorid,t1.operatorname,t1.isused,operpermission.operpermissionjson from sysinfo.operinfo t1 left join (select operatorid,array_to_json(array_agg(row_to_json(t))) operpermissionjson from (select  t1.operatorid,t1.permissiontype,t1.ifpermission,t1.permissionorder,t1.params,t1.sysactionid,t1.permissionid from sysinfo.operpermission t1 ) t group by t.operatorid) operpermission on t1.operatorid=operpermission.operatorid where t1.operatorid in (select operatorid from sysinfo.sysoper where systemid=(v_check->'info'->>'systemid')::integer) and (p_operatorid is null or p_operatorid=t1.operatorid)  order by t1.operatorid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.operinfopermission_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_pass character varying, p_tokenkey character varying, p_begintokentime timestamp without time zone, p_endtokentime timestamp without time zone, p_memo character varying, p_isused integer, p_mycode integer, p_upcode integer, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) OWNER TO gm;

--
-- Name: FUNCTION operinfopermission_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_pass character varying, p_tokenkey character varying, p_begintokentime timestamp without time zone, p_endtokentime timestamp without time zone, p_memo character varying, p_isused integer, p_mycode integer, p_upcode integer, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.operinfopermission_query(p_token character varying, p_rows integer, p_page integer, p_operatorid bigint, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_pass character varying, p_tokenkey character varying, p_begintokentime timestamp without time zone, p_endtokentime timestamp without time zone, p_memo character varying, p_isused integer, p_mycode integer, p_upcode integer, p_headimgurl text, p_nickname character varying, p_beginbirthday timestamp without time zone, p_endbirthday timestamp without time zone, p_tokentype smallint, p_tokeninterval integer) IS '查询员工
operatorid:员工编号
operatorname:员工姓名
sex:性别
phone:电话
accounts:帐号
pass:密码
tokenkey:令牌
begintokentime:令牌时间
endtokentime:令牌时间结束
memo:备注
isused:是否使用
mycode:推广码
upcode:推广上级id
headimgurl:头像url
nickname:昵称
beginbirthday:生日
endbirthday:生日结束
tokentype:令牌类型1默认单人登录2多人登录
tokeninterval:令牌时长默认180分钟
返回:{"operatorid":操作员id,"operatorname":操作员姓名,"operatorname":员工姓名,"sex":性别,"phone":电话,"accounts":帐号,"pass":密码,"tokenkey":令牌,"tokentime":令牌时间,"memo":备注,"isused":使用,"mycode":推广码,"upcode":推广上级id,"headimgurl":头像url,"nickname":昵称,"birthday":生日,"tokentype":令牌类型1默认单人登录2多人登录,"tokeninterval":令牌时长默认180分钟}';


--
-- Name: operinfopermission_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: postgres
--

CREATE FUNCTION sysinfo.operinfopermission_undel(p_token character varying, p_operatorid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,142);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.operinfo where operatorid=p_operatorid;
  if v_c=0 then return gm.returnjson(100011); end if;--操作员信息出错！

  update sysinfo.operinfo set isused=1,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where operatorid=p_operatorid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.operinfopermission_undel(p_token character varying, p_operatorid integer) OWNER TO postgres;


CREATE FUNCTION sysinfo.orgtype_add(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,120);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_orgtypeid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.orgtype_orgtypeid_seq');
      select count(*) into v_c from sysinfo.orgtype where orgtypeid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.orgtype where orgtypeid=p_orgtypeid;
    if v_c>0 then return gm.returnjson(100020); end if;--部门类型信息出错！
    v_id:=p_orgtypeid;
  end if;
  insert into sysinfo.orgtype( isused,orgtypeid,orgtypename,description,systemid) values(1,v_id,p_orgtypename,p_description,(v_check->'info'->>'systemid')::integer );
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.orgtype_add(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) OWNER TO gm;

--
-- Name: FUNCTION orgtype_add(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.orgtype_add(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) IS '新增部门类型
orgtypeid:部门类型编号
orgtypename:部门类型名
description:说明
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: orgtype_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.orgtype_del(p_token character varying, p_orgtypeid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,120);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.orgtype where orgtypeid=p_orgtypeid and systemid=(v_check->'info'->>'systemid')::integer and isused=1;
  if v_c=0 then return gm.returnjson(100020); end if;--部门类型信息出错！
  update sysinfo.orgtype set isused=0 where orgtypeid=p_orgtypeid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.orgtype_del(p_token character varying, p_orgtypeid integer) OWNER TO gm;

--
-- Name: FUNCTION orgtype_del(p_token character varying, p_orgtypeid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.orgtype_del(p_token character varying, p_orgtypeid integer) IS '删除部门类型
orgtypeid:id';


--
-- Name: orgtype_edit(character varying, integer, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.orgtype_edit(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,120);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  v_id:=p_orgtypeid;
  select count(*) into v_c from sysinfo.orgtype where orgtypeid=p_orgtypeid and systemid=(v_check->'info'->>'systemid')::integer and isused=1;
  if v_c=0 then return gm.returnjson(100020); end if;--部门类型信息出错！
  update sysinfo.orgtype set orgtypename=p_orgtypename,description=p_description,systemid=(v_check->'info'->>'systemid')::integer where orgtypeid=p_orgtypeid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.orgtype_edit(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) OWNER TO gm;

--
-- Name: FUNCTION orgtype_edit(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.orgtype_edit(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) IS '修改部门类型
orgtypeid:部门类型编号
orgtypename:部门类型名
description:说明
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: orgtype_merge(character varying, integer, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.orgtype_merge(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,120);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_orgtypeid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.orgtype_orgtypeid_seq');
      select count(*) into v_c from sysinfo.orgtype where orgtypeid=v_id;
    end loop;
  else
    v_id:=p_orgtypeid;
  end if;
  with "te" as (update sysinfo.orgtype set orgtypename=p_orgtypename,description=p_description,systemid=(v_check->'info'->>'systemid')::integer  where orgtypeid=v_id returning *)
  insert into sysinfo.orgtype ( isused,orgtypeid,orgtypename,description,systemid) select 1,v_id,p_orgtypename,p_description,(v_check->'info'->>'systemid')::integer  where (select count(*) from te) = 0;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.orgtype_merge(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) OWNER TO gm;

--
-- Name: FUNCTION orgtype_merge(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.orgtype_merge(p_token character varying, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) IS '合并部门类型
orgtypeid:部门类型编号
orgtypename:部门类型名
description:说明
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: orgtype_query(character varying, integer, integer, integer, integer, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.orgtype_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,120);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.orgtypeid,t1.orgtypename,t1.description from sysinfo.orgtype t1 where  (p_isused is null or p_isused=t1.isused) and (p_orgtypeid is null or p_orgtypeid=t1.orgtypeid) and (p_orgtypename is null or position(p_orgtypename in t1.orgtypename )>0) and (p_description is null or position(p_description in t1.description )>0) and t1.systemid=(v_check->'info'->>'systemid')::integer order by t1.orgtypeid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.orgtype t1 where  (p_isused is null or p_isused=t1.isused) and (p_orgtypeid is null or p_orgtypeid=t1.orgtypeid) and (p_orgtypename is null or position(p_orgtypename in t1.orgtypename )>0) and (p_description is null or position(p_description in t1.description )>0) and t1.systemid=(v_check->'info'->>'systemid')::integer;
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.orgtypeid,t1.orgtypename,t1.description from sysinfo.orgtype t1 where  (p_isused is null or p_isused=t1.isused) and (p_orgtypeid is null or p_orgtypeid=t1.orgtypeid) and (p_orgtypename is null or position(p_orgtypename in t1.orgtypename )>0) and (p_description is null or position(p_description in t1.description )>0) and t1.systemid=(v_check->'info'->>'systemid')::integer order by t1.orgtypeid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.orgtype_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) OWNER TO gm;

--
-- Name: FUNCTION orgtype_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_orgtypeid integer, p_orgtypename character varying, p_description character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.orgtype_query(p_token character varying, p_rows integer, p_page integer, p_isused integer, p_orgtypeid integer, p_orgtypename character varying, p_description character varying) IS '查询部门类型
isused:是否使用
orgtypeid:部门类型编号
orgtypename:部门类型名
description:说明
返回:{"total":总记录数,"rows":[{"isused":显示,"orgtypeid":部门类型编号,"orgtypename":部门类型名,"description":说明}]}';


--
-- Name: orgtype_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.orgtype_undel(p_token character varying, p_orgtypeid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,121);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.orgtype where orgtypeid=p_orgtypeid and systemid=(v_check->'info'->>'systemid')::integer and isused=0;
  if v_c=0 then return gm.returnjson(100020); end if;--部门类型信息出错！
  update sysinfo.orgtype set isused=1 where orgtypeid=p_orgtypeid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.orgtype_undel(p_token character varying, p_orgtypeid integer) OWNER TO gm;

--
-- Name: FUNCTION orgtype_undel(p_token character varying, p_orgtypeid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.orgtype_undel(p_token character varying, p_orgtypeid integer) IS '恢复部门类型
orgtypeid:id';


--
-- Name: province_add(character varying, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.province_add(p_token character varying, p_pid integer, p_province character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,182);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_pid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('public.province_pid_seq');
      select count(*) into v_c from public.province where pid=v_id;
    end loop;
  else
    select count(*) into v_c from public.province where pid=p_pid;
    if v_c>0 then return gm.returnjson(100033); end if;--省信息出错！
    v_id:=p_pid;
  end if;
  insert into public.province( pid,province,isused) values(v_id,p_province,1 );
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION public.province_add(p_token character varying, p_pid integer, p_province character varying) OWNER TO gm;

--
-- Name: FUNCTION province_add(p_token character varying, p_pid integer, p_province character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.province_add(p_token character varying, p_pid integer, p_province character varying) IS '新增省
pid:省id
province:省名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: province_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.province_del(p_token character varying, p_pid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,182);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from public.province where pid=p_pid;
  if v_c=0 then return gm.returnjson(100033); end if;--省信息出错！
  update public.province set isused=0 where pid=p_pid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION public.province_del(p_token character varying, p_pid integer) OWNER TO gm;

--
-- Name: FUNCTION province_del(p_token character varying, p_pid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.province_del(p_token character varying, p_pid integer) IS '删除省
pid:id';


--
-- Name: province_edit(character varying, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.province_edit(p_token character varying, p_pid integer, p_province character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,182);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  v_id:=p_pid;
  select count(*) into v_c from public.province where pid=p_pid;
  if v_c=0 then return gm.returnjson(100033); end if;--省信息出错！
  update public.province set province=p_province where pid=p_pid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION public.province_edit(p_token character varying, p_pid integer, p_province character varying) OWNER TO gm;

--
-- Name: FUNCTION province_edit(p_token character varying, p_pid integer, p_province character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.province_edit(p_token character varying, p_pid integer, p_province character varying) IS '修改省
pid:省id
province:省名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: province_merge(character varying, integer, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.province_merge(p_token character varying, p_pid integer, p_province character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,182);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_pid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('public.province_pid_seq');
      select count(*) into v_c from public.province where pid=v_id;
    end loop;
  else
    v_id:=p_pid;
  end if;
  with "te" as (update public.province set province=p_province  where pid=v_id returning *)
  insert into public.province ( pid,province,isused) select v_id,p_province,1  where (select count(*) from te) = 0;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION public.province_merge(p_token character varying, p_pid integer, p_province character varying) OWNER TO gm;

--
-- Name: FUNCTION province_merge(p_token character varying, p_pid integer, p_province character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.province_merge(p_token character varying, p_pid integer, p_province character varying) IS '合并省
pid:省id
province:省名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: province_query(character varying, integer, integer, integer, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.province_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_province character varying, p_isused integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,182);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.pid,t1.province,t1.isused from public.province t1 where  (p_pid is null or p_pid=t1.pid) and (p_province is null or position(p_province in t1.province )>0) and (p_isused is null or p_isused=t1.isused) order by t1.pid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from public.province t1 where  (p_pid is null or p_pid=t1.pid) and (p_province is null or position(p_province in t1.province )>0) and (p_isused is null or p_isused=t1.isused);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.pid,t1.province,t1.isused from public.province t1 where  (p_pid is null or p_pid=t1.pid) and (p_province is null or position(p_province in t1.province )>0) and (p_isused is null or p_isused=t1.isused) order by t1.pid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION public.province_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_province character varying, p_isused integer) OWNER TO gm;

--
-- Name: FUNCTION province_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_province character varying, p_isused integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.province_query(p_token character varying, p_rows integer, p_page integer, p_pid integer, p_province character varying, p_isused integer) IS '查询省
pid:省id
province:省名
isused:是否使用
返回:{"total":总记录数,"rows":[{"pid":省id,"province":省名,"isused":是否使用}]}';


--
-- Name: province_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION public.province_undel(p_token character varying, p_pid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,183);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from public.province where pid=p_pid;
  if v_c=0 then return gm.returnjson(100033); end if;--省信息出错！
  update public.province set isused=1 where pid=p_pid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION public.province_undel(p_token character varying, p_pid integer) OWNER TO gm;

--
-- Name: FUNCTION province_undel(p_token character varying, p_pid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION public.province_undel(p_token character varying, p_pid integer) IS '恢复省
pid:id';


--
-- Name: regaccount(character varying, character varying, smallint, character varying, character varying, character varying, integer, character varying, character varying, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.regaccount(p_code character varying, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_appid integer, p_nickname character varying, p_headimgurl character varying, p_system character varying, p_upcode character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c numeric;v_systemid int4;v_orgid int4;	v_id numeric;v_actionid varchar;v_pass varchar;v_return jsonb;v_mycode int;v_bool bool;v_unionid varchar;
BEGIN
	if p_system is null then 
	  v_systemid=100;
	else
	  select count(*) into v_c from sysinfo.systeminfo where loginname=p_system and isused=1;
		if v_c=1 then 
		  select systemid into v_systemid from sysinfo.systeminfo where loginname=p_system;
		else 
		  return gm.returnjson(100001); --帐号密码错误
		end if;
	end if;
  select isused into v_c from sysinfo.sysaction where actionid=171 and systemid=v_systemid;
	if v_c=0 then return gm.returnjson(100026);end if; --不能注册！
	if p_phone is null then 	  
		select count(*) into v_c from sysinfo.appparams where appid=p_appid and isused=1;
		if v_c=0 then return gm.returnjson(100026);end if; --不能注册！
		select accesstoken,tokentime+interval '2H'>now() into v_pass,v_bool from sysinfo.appparams where appid=p_appid and isused=1 and typeid=101;
		
		if v_pass is null then return gm.returnjson(100027);end if ;--注册码不正确！
		v_return:=gm.http_post('https://api.weixin.qq.com/wxa/business/getuserphonenumber?access_token='||v_pass||'&'::varchar,'{"code":"'||p_code||'"}','application/x-www-form-urlencoded');
		p_phone:=(v_return->>'phone_info')::json->>'purePhoneNumber';
	else
		-- 短信验证码：优先查 sms.sendcode（部署了 sms 扩展时）；
		-- 未部署 sms 时容错为 NULL → 返回 100027（注册码不正确），不再抛 schema 不存在异常
		BEGIN
		  select systemid into v_systemid from sms.sendcode where mobile=p_phone and code=p_code and expiretime>now() and actionid=136;
		EXCEPTION WHEN undefined_table THEN
		  v_systemid:=null;
		END;
    if v_systemid is null then return gm.returnjson(100027);end if ;--注册码不正确！
	end if;
  select 'https://api.weixin.qq.com/sns/jscode2session?grant_type=authorization_code&'||params||'&js_code='||p_accounts into v_pass from sysinfo.appparams where appid=p_appid;
	v_return:=gm.http_get(v_pass);
	if not v_return?'openid' then 
	  return gm.returnjson(100028,v_return::jsonb);--手机码不正确！
	end if;
	v_pass=v_return->>'openid';
	if v_return?'unionid' then 
	  v_unionid:=v_return->>'unionid';
	else
	  v_unionid:=null;
	end if;
	select count(*) into v_c from sysinfo.operaccounts where accounts=v_pass and typeid=101;
	if v_c>0 then return gm.returnjson(100029);end if;--微信号已注册！
  select array_to_json(array_agg(row_to_json(t))) into v_return from (select isused,operatorid from sysinfo.operinfo where phone=p_phone) t;
  if jsonb_array_length(v_return)>0 then
	    v_return:=v_return->0;
			v_id:=(v_return->>'operatorid')::integer;
      if v_return->>'isused'='0' then 
		    update sysinfo.operinfo set isused=1 where operatorid=v_id;
		  end if;
      insert into "sysinfo"."operaccounts" ("operatorid", "accounts", "appid", "typeid", "isused", "unionid") VALUES (v_id, v_pass, p_appid, 101, 1,v_unionid);
			insert into sysinfo.sysoper(operatorid,systemid) select v_id,v_systemid from sysinfo.sysoper where not(operatorid=v_id and systemid=v_systemid);
		  return gm.returnjson(0,json_build_object('id',v_id));
	end if;
	p_accounts:=p_phone;	
	v_c:=1;loop exit when (v_c=0);
	  v_mycode=trunc(random()*90000000+10000000);
	  select count(*) into v_c from sysinfo.operinfo where mycode=v_mycode;
	end loop;
  v_c:=1;loop exit when (v_c=0);
    v_id :=nextval('sysinfo.operinfo_operatorid_seq');
    select count(*) into v_c from sysinfo.operinfo where operatorid=v_id;
  end loop;
  insert into "sysinfo"."operaccounts" ("operatorid", "accounts", "appid", "typeid", "isused", "unionid") VALUES (v_id, v_pass, p_appid, 101, 1,v_unionid);
  insert into sysinfo.operinfo( operatorid,operatorname,sex,phone,accounts,pass,memo,isused,mycode,upcode,nickname,headimgurl) values(v_id,p_operatorname,p_sex,p_phone,coalesce(p_accounts,p_phone),null,p_memo,1,v_mycode,p_upcode,p_nickname,p_headimgurl);
  insert into sysinfo.sysoper(operatorid,systemid) select v_id,v_systemid from sysinfo.sysoper where not(operatorid=v_id and systemid=v_systemid);
  return gm.returnjson(0,json_build_object('id',v_id));
	
END
$$;


ALTER FUNCTION sysinfo.regaccount(p_code character varying, p_operatorname character varying, p_sex smallint, p_phone character varying, p_accounts character varying, p_memo character varying, p_appid integer, p_nickname character varying, p_headimgurl character varying, p_system character varying, p_upcode character varying) OWNER TO gm;

--
-- Name: roleinfo_add(character varying, integer, character varying, character varying, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.roleinfo_add(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,132);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_roleinfoid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.roleinfo_roleinfoid_seq');
      select count(*) into v_c from sysinfo.roleinfo where roleinfoid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.roleinfo where roleinfoid=p_roleinfoid;
    if v_c>0 then return gm.returnjson(100023); end if;--角色信息出错！
    v_id:=p_roleinfoid;
  end if;
  insert into sysinfo.rolepermission( roleinfoid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct v_id,nextval('sysinfo.rolepermission_permissionid_seq'),permissiontype,ifpermission,permissionorder,params,sysactionid  from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) t  where  (permissiontype=1 and sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (permissiontype=2 and sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer));
  insert into sysinfo.roleinfo( roleinfoid,roleinfoname,description,isused,systemid) values(v_id,p_roleinfoname,p_description,1,(v_check->'info'->>'systemid')::integer );
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.roleinfo_add(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION roleinfo_add(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.roleinfo_add(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) IS '新增角色
roleinfoid:角色编号
roleinfoname:角色名
description:描述
rolepermissionjson:角色权限{"permissionid":角色权限编号,"permissiontype":权限类型,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: roleinfo_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.roleinfo_del(p_token character varying, p_roleinfoid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,132);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.roleinfo where roleinfoid=p_roleinfoid and systemid=(v_check->'info'->>'systemid')::integer and isused=1;
  if v_c=0 then return gm.returnjson(100023); end if;--角色信息出错！
  delete from sysinfo.rolepermission where roleinfoid=p_roleinfoid;
  update sysinfo.roleinfo set isused=0 where roleinfoid=p_roleinfoid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.roleinfo_del(p_token character varying, p_roleinfoid integer) OWNER TO gm;

--
-- Name: FUNCTION roleinfo_del(p_token character varying, p_roleinfoid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.roleinfo_del(p_token character varying, p_roleinfoid integer) IS '删除角色
roleinfoid:id';


--
-- Name: roleinfo_edit(character varying, integer, character varying, character varying, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.roleinfo_edit(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,132);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  v_id:=p_roleinfoid;
  select count(*) into v_c from sysinfo.roleinfo where roleinfoid=p_roleinfoid and systemid=(v_check->'info'->>'systemid')::integer and isused=1;
  if v_c=0 then return gm.returnjson(100023); end if;--角色信息出错！
	with recursive tree as 
(select sysactionid from json_to_recordset(p_rolepermissionjson) as t(permissionid int4,permissiontype int2,ifpermission int2,permissionorder int4,params varchar,sysactionid int4) where  permissiontype=2
union 
 select t.sysactionid from sysinfo.rolepermission t inner join tree on t.roleinfoid=tree.sysactionid where permissiontype=2 
)	select count(*) into v_c from tree where sysactionid=p_roleinfoid;
  if v_c>0 then return gm.returnjson(100017);end if;--不能循环定义！
  -- 子表按 permissionid 增删改：先按提交主键差集删除、再按主键更新、最后插入新行
  -- （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）
  delete from sysinfo.rolepermission t where t.roleinfoid=p_roleinfoid and ((t.permissiontype=1 and t.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (t.permissiontype=2 and t.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer))) and not exists (select 1 from (select (elem->>'permissionid')::int4 as permissionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) s where s.permissionid=t.permissionid);
  update sysinfo.roleinfo set roleinfoname=p_roleinfoname,description=p_description,systemid=(v_check->'info'->>'systemid')::integer where roleinfoid=p_roleinfoid;
  update sysinfo.rolepermission t set permissiontype=s.permissiontype,ifpermission=s.ifpermission,permissionorder=s.permissionorder,params=s.params,sysactionid=s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) s where t.permissionid=s.permissionid and t.roleinfoid=p_roleinfoid and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  insert into sysinfo.rolepermission( roleinfoid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct p_roleinfoid,nextval('sysinfo.rolepermission_permissionid_seq'),s.permissiontype,s.ifpermission,s.permissionorder,s.params,s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) s where (s.permissionid is null or not exists (select 1 from sysinfo.rolepermission t2 where t2.permissionid=s.permissionid)) and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.roleinfo_edit(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION roleinfo_edit(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.roleinfo_edit(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) IS '修改角色
roleinfoid:角色编号
roleinfoname:角色名
description:描述
rolepermissionjson:角色权限{"permissionid":角色权限编号,"permissiontype":权限类型,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: roleinfo_merge(character varying, integer, character varying, character varying, json); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.roleinfo_merge(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,132);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_roleinfoid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.roleinfo_roleinfoid_seq');
      select count(*) into v_c from sysinfo.roleinfo where roleinfoid=v_id;
    end loop;
  else
    v_id:=p_roleinfoid;
  end if;
	with recursive tree as 
(select sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) t where  permissiontype=2
union 
 select t.sysactionid from sysinfo.rolepermission t inner join tree on t.roleinfoid=tree.sysactionid where permissiontype=2 
)	select count(*) into v_c from tree where sysactionid=v_id;
  if v_c>0 then return gm.returnjson(100017);end if;--不能循环定义！
  -- 子表按 permissionid 增删改：先按提交主键差集删除、再按主键更新、最后插入新行
  -- （delete 必须在 insert 之前，避免误删本次新插入、主键尚不在提交集合中的行）
  delete from sysinfo.rolepermission t where t.roleinfoid=v_id and ((t.permissiontype=1 and t.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (t.permissiontype=2 and t.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer))) and not exists (select 1 from (select (elem->>'permissionid')::int4 as permissionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) s where s.permissionid=t.permissionid);
  with "te" as (update sysinfo.roleinfo set roleinfoname=p_roleinfoname,description=p_description,systemid=(v_check->'info'->>'systemid')::integer where roleinfoid=v_id returning *)
  insert into sysinfo.roleinfo ( roleinfoid,roleinfoname,description,isused,systemid) select v_id,p_roleinfoname,p_description,1,(v_check->'info'->>'systemid')::integer where (select count(*) from te) = 0;
  update sysinfo.rolepermission t set permissiontype=s.permissiontype,ifpermission=s.ifpermission,permissionorder=s.permissionorder,params=s.params,sysactionid=s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) s where t.permissionid=s.permissionid and t.roleinfoid=v_id and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  insert into sysinfo.rolepermission( roleinfoid,permissionid,permissiontype,ifpermission,permissionorder,params,sysactionid) select distinct v_id,nextval('sysinfo.rolepermission_permissionid_seq'),s.permissiontype,s.ifpermission,s.permissionorder,s.params,s.sysactionid from (select (elem->>'permissionid')::int4 as permissionid,(elem->>'permissiontype')::int2 as permissiontype,(elem->>'ifpermission')::int2 as ifpermission,(elem->>'permissionorder')::int4 as permissionorder,(elem->>'params') as params,(elem->>'sysactionid')::int4 as sysactionid from jsonb_array_elements(p_rolepermissionjson::jsonb) as elem) s where (s.permissionid is null or not exists (select 1 from sysinfo.rolepermission t2 where t2.permissionid=s.permissionid)) and ((s.permissiontype=1 and s.sysactionid in (select sysactionid from sysinfo.sysaction where systemid=(v_check->'info'->>'systemid')::integer)) or (s.permissiontype=2 and s.sysactionid in (select roleinfoid from sysinfo.roleinfo where systemid=(v_check->'info'->>'systemid')::integer)));
  return gm.returnjson(0,json_build_object('id',v_id));
end;
$$;


ALTER FUNCTION sysinfo.roleinfo_merge(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) OWNER TO gm;

--
-- Name: FUNCTION roleinfo_merge(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.roleinfo_merge(p_token character varying, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_rolepermissionjson json) IS '合并角色
roleinfoid:角色编号
roleinfoname:角色名
description:描述
rolepermissionjson:角色权限{"permissionid":角色权限编号,"permissiontype":权限类型,"ifpermission":允许,"permissionorder":权限级别,"params":参数,"sysactionid":权限编号,
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: roleinfo_query(character varying, integer, integer, integer, character varying, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.roleinfo_query(p_token character varying, p_rows integer, p_page integer, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_isused integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,132);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.roleinfoid,t1.roleinfoname,t1.description,t1.isused,rolepermission.rolepermissionjson from sysinfo.roleinfo t1 left join (select roleinfoid,array_to_json(array_agg(row_to_json(t))) rolepermissionjson from (select  t1.roleinfoid,t1.permissionid,t1.permissiontype,t1.ifpermission,t1.permissionorder,t1.params,t1.sysactionid,case t1.permissiontype when 1 then t2.sysactionname else t3.roleinfoname end sysactionname from sysinfo.rolepermission t1  left join sysinfo.sysaction t2 on t2.sysactionid=t1.sysactionid left join sysinfo.roleinfo t3 on t3.roleinfoid=t1.sysactionid) t group by t.roleinfoid) rolepermission on t1.roleinfoid=rolepermission.roleinfoid where  (p_roleinfoid is null or p_roleinfoid=t1.roleinfoid) and (p_roleinfoname is null or position(p_roleinfoname in t1.roleinfoname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_isused is null or p_isused=t1.isused) and t1.systemid=(v_check->'info'->>'systemid')::integer order by t1.roleinfoid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.roleinfo t1 where  (p_roleinfoid is null or p_roleinfoid=t1.roleinfoid) and (p_roleinfoname is null or position(p_roleinfoname in t1.roleinfoname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_isused is null or p_isused=t1.isused) and t1.systemid=(v_check->'info'->>'systemid')::integer;
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.roleinfoid,t1.roleinfoname,t1.description,t1.isused,rolepermission.rolepermissionjson from sysinfo.roleinfo t1 left join (select roleinfoid,array_to_json(array_agg(row_to_json(t))) rolepermissionjson from (select  t1.roleinfoid,t1.permissionid,t1.permissiontype,t1.ifpermission,t1.permissionorder,t1.params,t1.sysactionid,case t1.permissiontype when 1 then t2.sysactionname else t3.roleinfoname end sysactionname from sysinfo.rolepermission t1  left join sysinfo.sysaction t2 on t2.sysactionid=t1.sysactionid left join sysinfo.roleinfo t3 on t3.roleinfoid=t1.sysactionid) t group by t.roleinfoid) rolepermission on t1.roleinfoid=rolepermission.roleinfoid where  (p_roleinfoid is null or p_roleinfoid=t1.roleinfoid) and (p_roleinfoname is null or position(p_roleinfoname in t1.roleinfoname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_isused is null or p_isused=t1.isused) and t1.systemid=(v_check->'info'->>'systemid')::integer order by t1.roleinfoid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.roleinfo_query(p_token character varying, p_rows integer, p_page integer, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_isused integer) OWNER TO gm;

--
-- Name: FUNCTION roleinfo_query(p_token character varying, p_rows integer, p_page integer, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_isused integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.roleinfo_query(p_token character varying, p_rows integer, p_page integer, p_roleinfoid integer, p_roleinfoname character varying, p_description character varying, p_isused integer) IS '查询角色
roleinfoid:角色编号
roleinfoname:角色名
description:描述
isused:是否使用
返回:{"total":总记录数,"rows":[{"roleinfoid":角色编号,"roleinfoname":角色名,"description":描述,"isused":显示}]}';


--
-- Name: roleinfo_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.roleinfo_undel(p_token character varying, p_roleinfoid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,133);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.roleinfo where roleinfoid=p_roleinfoid and systemid=(v_check->'info'->>'systemid')::integer and isused=0;
  if v_c=0 then return gm.returnjson(100023); end if;--角色信息出错！

  update sysinfo.roleinfo set isused=1 where roleinfoid=p_roleinfoid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.roleinfo_undel(p_token character varying, p_roleinfoid integer) OWNER TO gm;

--
-- Name: FUNCTION roleinfo_undel(p_token character varying, p_roleinfoid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.roleinfo_undel(p_token character varying, p_roleinfoid integer) IS '恢复角色
roleinfoid:id';


--
-- Name: loginlog_query(character varying, integer, integer, bigint, timestamp without time zone, timestamp without time zone, bigint, inet, character varying, smallint); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.loginlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_beginlogintime timestamp without time zone, p_endlogintime timestamp without time zone, p_operatorno bigint, p_ip inet, p_accounts character varying, p_systemid smallint) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,107);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select t1.logid,t1.logintime::varchar,t1.operatorno,t1.ip::varchar,t1.accounts,t1.systemid,toperator.operatorname from public.loginlog t1 left join sysinfo.operinfo toperator on t1.operatorno=toperator.operatorid where (p_logid is null or p_logid=t1.logid) and (p_beginlogintime is null or t1.logintime>=p_beginlogintime) and (p_endlogintime is null or t1.logintime<=p_endlogintime) and (p_operatorno is null or p_operatorno=t1.operatorno) and (p_ip is null or p_ip=t1.ip) and (p_accounts is null or position(p_accounts in t1.accounts)>0) and (p_systemid is null or p_systemid=t1.systemid) order by t1.logid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from public.loginlog t1 where (p_logid is null or p_logid=t1.logid) and (p_beginlogintime is null or t1.logintime>=p_beginlogintime) and (p_endlogintime is null or t1.logintime<=p_endlogintime) and (p_operatorno is null or p_operatorno=t1.operatorno) and (p_ip is null or p_ip=t1.ip) and (p_accounts is null or position(p_accounts in t1.accounts)>0) and (p_systemid is null or p_systemid=t1.systemid);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select t1.logid,t1.logintime::varchar,t1.operatorno,t1.ip::varchar,t1.accounts,t1.systemid,toperator.operatorname from public.loginlog t1 left join sysinfo.operinfo toperator on t1.operatorno=toperator.operatorid where (p_logid is null or p_logid=t1.logid) and (p_beginlogintime is null or t1.logintime>=p_beginlogintime) and (p_endlogintime is null or t1.logintime<=p_endlogintime) and (p_operatorno is null or p_operatorno=t1.operatorno) and (p_ip is null or p_ip=t1.ip) and (p_accounts is null or position(p_accounts in t1.accounts)>0) and (p_systemid is null or p_systemid=t1.systemid) order by t1.logid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.loginlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_beginlogintime timestamp without time zone, p_endlogintime timestamp without time zone, p_operatorno bigint, p_ip inet, p_accounts character varying, p_systemid smallint) OWNER TO gm;

COMMENT ON FUNCTION sysinfo.loginlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_beginlogintime timestamp without time zone, p_endlogintime timestamp without time zone, p_operatorno bigint, p_ip inet, p_accounts character varying, p_systemid smallint) IS '查询登录日志（public.loginlog：登录成功/失败均记录，不含口令；分页同 serverlog_query）
logid:日志ID
beginlogintime:登录时间起
endlogintime:登录时间止
operatorno:操作员ID
ip:登录IP
accounts:账号
systemid:子系统ID
返回:{"total":总记录数,"rows":[{"logid":日志ID,"logintime":登录时间,"operatorno":操作员ID,"operatorname":操作员姓名,"ip":登录IP,"accounts":账号,"systemid":子系统ID}]}';


--
-- Name: serverlog_query(character varying, integer, integer, bigint, inet, character varying, character varying, text, timestamp without time zone, timestamp without time zone, inet, text, bigint, text, text); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.serverlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_clientip inet, p_pckname character varying, p_funcname character varying, p_content text, p_beginlogtime timestamp without time zone, p_endlogtime timestamp without time zone, p_serverip inet, p_params text, p_operatorid bigint, p_res text, p_head text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,107);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.logid,t1.clientip,t1.pckname,t1.funcname,left(t1.content,2000) content,t1.logtime::varchar,t1.serverip,left(t1.params,2000) params,t1.operatorid,toperatorid.operatorname operatoridname,left(t1.res,2000) res,left(t1.head,2000) head from public.serverlog t1 left join sysinfo.operinfo toperatorid on t1.operatorid=toperatorid.operatorid where  (p_logid is null or p_logid=t1.logid) and (p_clientip is null or p_clientip=t1.clientip) and (p_pckname is null or position(p_pckname in t1.pckname )>0) and (p_funcname is null or position(p_funcname in t1.funcname )>0) and (p_content is null or position(p_content in t1.content )>0) and (p_beginlogtime is null or t1.logtime>=p_beginlogtime) and (p_endlogtime is null or t1.logtime<=p_endlogtime) and (p_serverip is null or p_serverip=t1.serverip) and (p_params is null or position(p_params in t1.params )>0) and (p_operatorid is null or p_operatorid=t1.operatorid) and (p_res is null or position(p_res in t1.res )>0) and (p_head is null or position(p_head in t1.head )>0) order by t1.logid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from public.serverlog t1 where  (p_logid is null or p_logid=t1.logid) and (p_clientip is null or p_clientip=t1.clientip) and (p_pckname is null or position(p_pckname in t1.pckname )>0) and (p_funcname is null or position(p_funcname in t1.funcname )>0) and (p_content is null or position(p_content in t1.content )>0) and (p_beginlogtime is null or t1.logtime>=p_beginlogtime) and (p_endlogtime is null or t1.logtime<=p_endlogtime) and (p_serverip is null or p_serverip=t1.serverip) and (p_params is null or position(p_params in t1.params )>0) and (p_operatorid is null or p_operatorid=t1.operatorid) and (p_res is null or position(p_res in t1.res )>0) and (p_head is null or position(p_head in t1.head )>0);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.logid,t1.clientip,t1.pckname,t1.funcname,left(t1.content,2000) content,t1.logtime::varchar,t1.serverip,left(t1.params,2000) params,t1.operatorid,toperatorid.operatorname operatoridname,left(t1.res,2000) res,left(t1.head,2000) head from public.serverlog t1 left join sysinfo.operinfo toperatorid on t1.operatorid=toperatorid.operatorid where  (p_logid is null or p_logid=t1.logid) and (p_clientip is null or p_clientip=t1.clientip) and (p_pckname is null or position(p_pckname in t1.pckname )>0) and (p_funcname is null or position(p_funcname in t1.funcname )>0) and (p_content is null or position(p_content in t1.content )>0) and (p_beginlogtime is null or t1.logtime>=p_beginlogtime) and (p_endlogtime is null or t1.logtime<=p_endlogtime) and (p_serverip is null or p_serverip=t1.serverip) and (p_params is null or position(p_params in t1.params )>0) and (p_operatorid is null or p_operatorid=t1.operatorid) and (p_res is null or position(p_res in t1.res )>0) and (p_head is null or position(p_head in t1.head )>0) order by t1.logid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.serverlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_clientip inet, p_pckname character varying, p_funcname character varying, p_content text, p_beginlogtime timestamp without time zone, p_endlogtime timestamp without time zone, p_serverip inet, p_params text, p_operatorid bigint, p_res text, p_head text) OWNER TO gm;

--
-- Name: FUNCTION serverlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_clientip inet, p_pckname character varying, p_funcname character varying, p_content text, p_beginlogtime timestamp without time zone, p_endlogtime timestamp without time zone, p_serverip inet, p_params text, p_operatorid bigint, p_res text, p_head text); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.serverlog_query(p_token character varying, p_rows integer, p_page integer, p_logid bigint, p_clientip inet, p_pckname character varying, p_funcname character varying, p_content text, p_beginlogtime timestamp without time zone, p_endlogtime timestamp without time zone, p_serverip inet, p_params text, p_operatorid bigint, p_res text, p_head text) IS '查询系统日志（public.serverlog：模块请求日志 + 操作审计(pckname=audit)；分页：p_rows 每页行数+p_page 页码，均空返回全量）
logid:日志ID
clientip:客户IP
pckname:包名(audit=操作审计)
funcname:功能名
content:内容
beginlogtime:时间
endlogtime:时间结束
serverip:服务器IP
params:参数
operatorid:操作员
res:返回
head:头
返回:{"total":总记录数,"rows":[{"logid":日志ID,"clientip":客户IP,"pckname":包名,"funcname":功能名,"content":内容,"logtime":时间,"serverip":服务器IP,"params":参数,"operatorid":操作员id,"operatorname":操作员姓名,"res":返回,"head":头}]}';


--
-- Name: sysaction_add(character varying, integer, character varying, integer, integer, character varying, integer, smallint); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysaction_add(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,108);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_upid is not null then
    select count(*) into v_c from sysinfo.sysaction where sysactionid=p_upid and isused<>0;
    if v_c=0 then return gm.returnjson(100014);end if;--上级编号不存在
  end if;
  if p_sysactionid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id:=nextval('sysinfo.sysaction_sysactionid_seq');
      select count(*) into v_c from sysinfo.sysaction where sysactionid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.sysaction where sysactionid=p_sysactionid;
    if v_c>0 then return gm.returnjson(100013); end if;--系统权限信息出错！
    v_id:=p_sysactionid;
  end if;
  insert into sysinfo.sysaction( upid,isused,params,systemid,sysactionid,sysactionname,actionid,isdefault) values(coalesce(p_upid,0),1,p_params,p_systemid,v_id,p_sysactionname,p_actionid,p_isdefault );
  with recursive tree as
  ( select sysactionid,upid,1 as idlevel,coalesce(actionid,sysactionid)::varchar as idpath,t1.systemid from sysinfo.sysaction t1 where upid=0
  union
  select t1.sysactionid,t1.upid,t2.idlevel+1,t2.idpath||'.'||coalesce(t1.actionid,t1.sysactionid),t1.systemid from sysinfo.sysaction t1 inner join tree t2 on t2.sysactionid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysaction t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysaction where isused<>0 group by upid,systemid) t2 on t1.sysactionid=t2.upid and t1.systemid=t2.systemid
  where t.sysactionid=t1.sysactionid;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.sysaction_add(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) OWNER TO gm;

--
-- Name: FUNCTION sysaction_add(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysaction_add(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) IS '新增系统权限
upid:上级id
params:参数
systemid:系统编号
sysactionid:系统权限编号
sysactionname:权限名
actionid:权限编号
isdefault:是否默认
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id} }';


--
-- Name: sysaction_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysaction_del(p_token character varying, p_sysactionid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_check json;
begin
  v_check:=gm.check_login(p_token,111);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.sysaction where sysactionid=p_sysactionid;
  if v_c=0 then return gm.returnjson(100013); end if;--系统权限信息出错！
  select count(*) into v_c from sysinfo.sysaction where upid=p_sysactionid and isused<>0;
  if v_c>0 then return gm.returnjson(100016);end if;--有下级类型不能删除
  update sysinfo.sysaction set isused=0 where sysactionid=p_sysactionid;
  with recursive tree as
  ( select sysactionid,upid,1 as idlevel,sysactionid::varchar as idpath,t1.systemid from sysinfo.sysaction t1 where upid=0
  union
  select t1.sysactionid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysactionid,t1.systemid from sysinfo.sysaction t1 inner join tree t2 on t2.sysactionid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysaction t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysaction where isused<>0 group by upid,systemid) t2 on t1.sysactionid=t2.upid
  where t.sysactionid=t1.sysactionid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.sysaction_del(p_token character varying, p_sysactionid integer) OWNER TO gm;

--
-- Name: FUNCTION sysaction_del(p_token character varying, p_sysactionid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysaction_del(p_token character varying, p_sysactionid integer) IS '删除系统权限
sysactionid:id';


--
-- Name: sysaction_edit(character varying, integer, character varying, integer, character varying, integer, integer, smallint); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysaction_edit(p_token character varying, p_upid integer, p_params character varying, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_systemid integer, p_isdefault smallint) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_idpath varchar;v_check json;
begin
  v_check:=gm.check_login(p_token,109);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.sysaction where sysactionid=p_sysactionid and isused<>0;
  if v_c=0 then return gm.returnjson(100013); end if;--系统权限信息出错！
	if p_actionid is not null then 
	  select count(*) into v_c from sysinfo.actions where actionid=p_actionid;
	  if v_c=0 then return gm.returnjson(100013); end if;--系统权限信息出错！
	end if;
	select count(*) into v_c from sysinfo.systeminfo where systemid=p_systemid;
  if v_c=0 then return gm.returnjson(100012);end if;--子系统信息出错！
  if p_upid is not null then
    select count(*) into v_c from sysinfo.sysaction where sysactionid=p_upid and isused<>0;
    if v_c<>1 then return gm.returnjson(100014);end if;--上级不存在
    select idpath into v_idpath from sysinfo.sysaction where sysactionid=p_upid and isused<>0;
    if position('.'||p_sysactionid||'.' in '.'||v_idpath||'.')>0 then return gm.returnjson(100017);end if;--上级机构不能循环定义
  end if;
	if p_isdefault<>0 then p_isdefault:=1;end if;
	update sysinfo.sysaction set upid=coalesce(p_upid,0),params=p_params,systemid=p_systemid,sysactionname=p_sysactionname,actionid=p_actionid,isdefault=p_isdefault  where sysactionid=p_sysactionid;
  with recursive tree as
  ( select sysactionid,upid,1 as idlevel,coalesce(actionid,sysactionid)::varchar as idpath,t1.systemid from sysinfo.sysaction t1 where upid=0
  union
  select t1.sysactionid,t1.upid,t2.idlevel+1,t2.idpath||'.'||coalesce(t1.actionid,t1.sysactionid),t1.systemid from sysinfo.sysaction t1 inner join tree t2 on t2.sysactionid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysaction t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysaction where isused<>0 group by upid,systemid) t2 on t1.sysactionid=t2.upid and t1.systemid=t2.systemid
  where t.sysactionid=t1.sysactionid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.sysaction_edit(p_token character varying, p_upid integer, p_params character varying, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_systemid integer, p_isdefault smallint) OWNER TO gm;

--
-- Name: FUNCTION sysaction_edit(p_token character varying, p_upid integer, p_params character varying, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_systemid integer, p_isdefault smallint); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysaction_edit(p_token character varying, p_upid integer, p_params character varying, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_systemid integer, p_isdefault smallint) IS '修改系统权限
params:参数
sysactionid:系统权限编号
sysactionname:权限名
actionid:权限编号
systemid:系统编号
isdeault:是否默认,默认是
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: sysaction_merge(character varying, integer, character varying, integer, integer, character varying, integer, smallint); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysaction_merge(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_id integer;v_idpath varchar;v_check json;
begin
  v_check:=gm.check_login(p_token,110);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_upid is not null then
    select count(*) into v_c from sysinfo.sysaction where sysactionid=p_upid and isused<>0;
    if v_c<>1 then return gm.returnjson(100014);end if;--上级不存在
    select idpath into v_idpath from sysinfo.sysaction where sysactionid=p_upid and isused<>0;
    if position('.'||p_sysactionid||'.' in '.'||v_idpath||'.')>0 then return gm.returnjson(100017);end if;--上级机构不能循环定义
  end if;
  if p_sysactionid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.sysaction_sysactionid_seq');
      select count(*) into v_c from sysinfo.sysaction where sysactionid=v_id;
    end loop;
  else
    v_id:=p_sysactionid;
  end if;
  if position('.'||p_sysactionid||'.' in '.'||v_idpath||'.')>0 then return gm.returnjson(100033);end if;--上级机构不能循环定义
  with "te" as (update sysinfo.sysaction set upid=coalesce(p_upid,100),params=p_params,systemid=p_systemid,sysactionname=p_sysactionname,actionid=p_actionid,isdefault=p_isdefault  where sysactionid=p_sysactionid returning *)
  insert into sysinfo.sysaction ( upid,isused,params,systemid,sysactionid,sysactionname,actionid,isdefault) select coalesce(p_upid,100),1,p_params,p_systemid,v_id,p_sysactionname,p_actionid,p_isdefault  where (select count(*) from te) = 0;
  with recursive tree as
  ( select sysactionid,upid,1 as idlevel,sysactionid::varchar as idpath,t1.systemid from sysinfo.sysaction t1 where upid=0
  union
  select t1.sysactionid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysactionid,t1.systemid from sysinfo.sysaction t1 inner join tree t2 on t2.sysactionid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysaction t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysaction where isused<>0 group by upid,systemid) t2 on t1.sysactionid=t2.upid
  where t.sysactionid=t1.sysactionid;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.sysaction_merge(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) OWNER TO gm;

--
-- Name: FUNCTION sysaction_merge(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysaction_merge(p_token character varying, p_upid integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) IS '合并系统权限
upid:上级id
params:参数
systemid:系统编号
sysactionid:系统权限编号
sysactionname:权限名
actionid:权限编号
isdefault:是否默认
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id} }';


--
-- Name: sysaction_query(character varying, integer, integer, integer, character varying, integer, character varying, integer, integer, character varying, integer, smallint); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysaction_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,113);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.upid,t1.isused,t1.params,t1.systemid,ts.systemname,t1.sysactionid,t1.sysactionname,t1.actionid,t1.isdefault,t1.idpath,t1.idlevel,tt.sysactionname upname from sysinfo.sysaction t1 left join sysinfo.sysaction tt on t1.upid=tt.sysactionid  left join sysinfo.systeminfo ts on t1.systemid=ts.systemid where (p_isused is null or t1.isused=p_isused) and (p_upid is null or p_upid=t1.upid) and (p_idpath is null or position(p_idpath in t1.idpath)>0)  and (p_isused is null or p_isused=t1.isused) and (p_params is null or position(p_params in t1.params )>0) and (p_systemid is null or t1.systemid=p_systemid) and (p_sysactionid is null or p_sysactionid=t1.sysactionid) and (p_sysactionname is null or position(p_sysactionname in t1.sysactionname )>0) and (p_actionid is null or p_actionid=t1.actionid) and (p_isdefault is null or p_isdefault=t1.isdefault) order by t1.idpath ) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.sysaction t1 where (p_isused is null or t1.isused=p_isused) and (p_upid is null or p_upid=t1.upid) and (p_idpath is null or position(p_idpath in t1.idpath)>0)  and (p_isused is null or p_isused=t1.isused) and (p_params is null or position(p_params in t1.params )>0) and (p_systemid is null or t1.systemid=p_systemid) and (p_sysactionid is null or p_sysactionid=t1.sysactionid) and (p_sysactionname is null or position(p_sysactionname in t1.sysactionname )>0) and (p_actionid is null or p_actionid=t1.actionid) and (p_isdefault is null or p_isdefault=t1.isdefault);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.upid,t1.isused,t1.params,t1.systemid,ts.systemname,t1.sysactionid,t1.sysactionname,t1.actionid,t1.isdefault,t1.idpath,t1.idlevel,tt.sysactionname upname from sysinfo.sysaction t1 left join sysinfo.sysaction tt on t1.upid=tt.sysactionid  left join sysinfo.systeminfo ts on t1.systemid=ts.systemid where (p_isused is null or t1.isused=p_isused) and (p_upid is null or p_upid=t1.upid) and (p_idpath is null or position(p_idpath in t1.idpath)>0)  and (p_isused is null or p_isused=t1.isused) and (p_params is null or position(p_params in t1.params )>0) and (p_systemid is null or t1.systemid=p_systemid) and (p_sysactionid is null or p_sysactionid=t1.sysactionid) and (p_sysactionname is null or position(p_sysactionname in t1.sysactionname )>0) and (p_actionid is null or p_actionid=t1.actionid) and (p_isdefault is null or p_isdefault=t1.isdefault) order by t1.idpath limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.sysaction_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) OWNER TO gm;

--
-- Name: FUNCTION sysaction_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysaction_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_params character varying, p_systemid integer, p_sysactionid integer, p_sysactionname character varying, p_actionid integer, p_isdefault smallint) IS '查询系统权限
upid:上级id
idpath:包含节点
isused:是否使用
params:参数
systemid:系统编号
sysactionid:系统权限编号
sysactionname:权限名
actionid:权限编号
isdefault:是否默认
返回:{"total":总记录数,"rows":[{"upid":上级id,"isused":使用,"params":参数,"systemid":系统id,"systemname":系统名称,"sysactionid":系统权限编号,"sysactionname":权限名,"actionid":权限编号,"isdefault":是否默认}]}';


--
-- Name: sysaction_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysaction_undel(p_token character varying, p_sysactionid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_check json;
begin
  v_check:=gm.check_login(p_token,112);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_sysactionid=100 then return gm.returnjson(100015);end if; --顶级
  select count(*) into v_c from sysinfo.sysaction where sysactionid=p_sysactionid;
  if v_c=0 then return gm.returnjson(100013); end if;--系统权限信息出错！
  select count(*) into v_c from sysinfo.sysaction where upid=p_sysactionid and isused<>0;
  if v_c>0 then return gm.returnjson(100016);end if;--有下级类型不能删除
  update sysinfo.sysaction set isused=1 where sysactionid=p_sysactionid;
  with recursive tree as
  ( select sysactionid,upid,1 as idlevel,sysactionid::varchar as idpath,t1.systemid from sysinfo.sysaction t1 where upid=0
  union
  select t1.sysactionid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysactionid,t1.systemid from sysinfo.sysaction t1 inner join tree t2 on t2.sysactionid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysaction t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysaction where isused<>0 group by upid,systemid) t2 on t1.sysactionid=t2.upid
  where t.sysactionid=t1.sysactionid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.sysaction_undel(p_token character varying, p_sysactionid integer) OWNER TO gm;

--
-- Name: FUNCTION sysaction_undel(p_token character varying, p_sysactionid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysaction_undel(p_token character varying, p_sysactionid integer) IS '恢复系统权限
sysactionid:id';


--
-- Name: sysorg_add(character varying, integer, integer, character varying, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysorg_add(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,114);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_upid is not null then
    select count(*) into v_c from sysinfo.sysorg where sysorgid=p_upid and isused<>0;
    if v_c=0 then return gm.returnjson(100014);end if;--上级编号不存在
  end if;
  if p_sysorgid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id:=nextval('sysinfo.sysorg_sysorgid_seq');
      select count(*) into v_c from sysinfo.sysorg where sysorgid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.sysorg where sysorgid=p_sysorgid;
    if v_c>0 then return gm.returnjson(100018); end if;--部门信息出错！
    v_id:=p_sysorgid;
  end if;
  insert into sysinfo.sysorg( upid,isused,systemid,sysorgid,sysorgname,description,orgtype) values(coalesce(p_upid,100),1,(v_check->'info'->>'systemid')::integer,v_id,p_sysorgname,p_description,p_orgtype );
  with recursive tree as
  ( select sysorgid,upid,1 as idlevel,sysorgid::varchar as idpath,t1.systemid from sysinfo.sysorg t1 where coalesce(upid,0)=0
  union
  select t1.sysorgid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysorgid,t1.systemid from sysinfo.sysorg t1 inner join tree t2 on t2.sysorgid=t1.upid and t2.systemid=t1.systemid where isused<>0)
  update sysinfo.sysorg t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysorg where isused<>0 group by upid,systemid) t2 on t1.sysorgid=t2.upid
  where t.sysorgid=t1.sysorgid;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.sysorg_add(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) OWNER TO gm;

--
-- Name: FUNCTION sysorg_add(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysorg_add(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) IS '新增部门
upid:上级id
sysorgid:部门id
sysorgname:部门名称
description:说明
orgtype:部门类型id
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id} }';


--
-- Name: sysorg_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysorg_del(p_token character varying, p_sysorgid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_check json;
begin
  v_check:=gm.check_login(p_token,117);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.sysorg where sysorgid=p_sysorgid;
  if v_c=0 then return gm.returnjson(100018); end if;--部门信息出错！
  select count(*) into v_c from sysinfo.sysorg where upid=p_sysorgid and isused<>0;
  if v_c>0 then return gm.returnjson(100016);end if;--有下级类型不能删除
  update sysinfo.sysorg set isused=0 where sysorgid=p_sysorgid;
  with recursive tree as
  ( select sysorgid,upid,1 as idlevel,sysorgid::varchar as idpath,t1.systemid from sysinfo.sysorg t1 where coalesce(upid,0)=0
  union
  select t1.sysorgid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysorgid,t1.systemid from sysinfo.sysorg t1 inner join tree t2 on t2.sysorgid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysorg t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysorg where isused<>0 group by upid,systemid) t2 on t1.sysorgid=t2.upid
  where t.sysorgid=t1.sysorgid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.sysorg_del(p_token character varying, p_sysorgid integer) OWNER TO gm;

--
-- Name: FUNCTION sysorg_del(p_token character varying, p_sysorgid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysorg_del(p_token character varying, p_sysorgid integer) IS '删除部门
sysorgid:id';


--
-- Name: sysorg_edit(character varying, integer, integer, character varying, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysorg_edit(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_idpath varchar;v_check json;
begin
  v_check:=gm.check_login(p_token,115);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.sysorg where sysorgid=p_sysorgid and isused<>0;
  if v_c=0 then return gm.returnjson(100018); end if;--部门信息出错！
  if p_upid is not null then
    select count(*) into v_c from sysinfo.sysorg where sysorgid=p_upid and isused<>0;
    if v_c<>1 then return gm.returnjson(100014);end if;--上级不存在
    if p_upid=p_sysorgid then return gm.returnjson(100017);end if;--上级不能是自身
    select idpath into v_idpath from sysinfo.sysorg where sysorgid=p_upid and isused<>0;
    if position('.'||p_sysorgid||'.' in '.'||v_idpath||'.')>0 then return gm.returnjson(100017);end if;--上级机构不能循环定义
  end if;
  update sysinfo.sysorg set upid=coalesce(p_upid,100),systemid=(v_check->'info'->>'systemid')::integer,sysorgname=p_sysorgname,description=p_description,orgtype=p_orgtype  where sysorgid=p_sysorgid;
  with recursive tree as
  ( select sysorgid,upid,1 as idlevel,sysorgid::varchar as idpath,t1.systemid from sysinfo.sysorg t1 where coalesce(upid,0)=0
  union
  select t1.sysorgid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysorgid,t1.systemid from sysinfo.sysorg t1 inner join tree t2 on t2.sysorgid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysorg t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysorg where isused<>0 group by upid,systemid) t2 on t1.sysorgid=t2.upid
  where t.sysorgid=t1.sysorgid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.sysorg_edit(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) OWNER TO gm;

--
-- Name: FUNCTION sysorg_edit(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysorg_edit(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) IS '修改部门
upid:上级id
sysorgid:部门id
sysorgname:部门名称
description:说明
orgtype:部门类型id
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id} }';


--
-- Name: sysorg_merge(character varying, integer, integer, character varying, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysorg_merge(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_id integer;v_idpath varchar;v_check json;
begin
  v_check:=gm.check_login(p_token,116);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_upid is not null then
    select count(*) into v_c from sysinfo.sysorg where sysorgid=p_upid and isused<>0;
    if v_c<>1 then return gm.returnjson(100014);end if;--上级不存在
    if p_upid=p_sysorgid then return gm.returnjson(100017);end if;--上级不能是自身
    select idpath into v_idpath from sysinfo.sysorg where sysorgid=p_upid and isused<>0;
    if position('.'||p_sysorgid||'.' in '.'||v_idpath||'.')>0 then return gm.returnjson(100017);end if;--上级机构不能循环定义
  end if;
  if p_sysorgid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.sysorg_sysorgid_seq');
      select count(*) into v_c from sysinfo.sysorg where sysorgid=v_id;
    end loop;
  else
    v_id:=p_sysorgid;
  end if;
  if position('.'||p_sysorgid||'.' in '.'||v_idpath||'.')>0 then return gm.returnjson(100033);end if;--上级机构不能循环定义
  with "te" as (update sysinfo.sysorg set upid=coalesce(p_upid,100),systemid=(v_check->'info'->>'systemid')::integer,sysorgname=p_sysorgname,description=p_description,orgtype=p_orgtype  where sysorgid=p_sysorgid returning *)
  insert into sysinfo.sysorg ( upid,isused,systemid,sysorgid,sysorgname,description,orgtype) select coalesce(p_upid,100),1,(v_check->'info'->>'systemid')::integer,v_id,p_sysorgname,p_description,p_orgtype  where (select count(*) from te) = 0;
  with recursive tree as
  ( select sysorgid,upid,1 as idlevel,sysorgid::varchar as idpath,t1.systemid from sysinfo.sysorg t1 where coalesce(upid,0)=0
  union
  select t1.sysorgid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysorgid,t1.systemid from sysinfo.sysorg t1 inner join tree t2 on t2.sysorgid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysorg t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysorg where isused<>0 group by upid,systemid) t2 on t1.sysorgid=t2.upid
  where t.sysorgid=t1.sysorgid;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.sysorg_merge(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) OWNER TO gm;

--
-- Name: FUNCTION sysorg_merge(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysorg_merge(p_token character varying, p_upid integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) IS '合并部门
upid:上级id
sysorgid:部门id
sysorgname:部门名称
description:说明
orgtype:部门类型id
返回：{"errorcode":0,"message":"执行成功！","info":{"id":id} }';


--
-- Name: sysorg_query(character varying, integer, integer, integer, character varying, integer, integer, character varying, character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysorg_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,119);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.upid,t1.isused,t1.sysorgid,t1.sysorgname,t1.description,t1.orgtype,t2.orgtypename,t1.idpath,t1.idlevel,tt.sysorgname upname from sysinfo.sysorg t1 left join sysinfo.sysorg tt on t1.upid=tt.sysorgid  left join sysinfo.orgtype t2 on t2.orgtypeid=t1.orgtype where (p_isused is null or t1.isused=p_isused) and (p_upid is null or p_upid=t1.upid) and (p_idpath is null or position(p_idpath in t1.idpath)>0)  and (p_isused is null or p_isused=t1.isused) and t1.systemid=(v_check->'info'->>'systemid')::integer and (p_sysorgid is null or p_sysorgid=t1.sysorgid) and (p_sysorgname is null or position(p_sysorgname in t1.sysorgname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_orgtype is null or p_orgtype=t1.orgtype) order by t1.idpath ) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.sysorg t1 where (p_isused is null or t1.isused=p_isused) and (p_upid is null or p_upid=t1.upid) and (p_idpath is null or position(p_idpath in t1.idpath)>0)  and (p_isused is null or p_isused=t1.isused) and t1.systemid=(v_check->'info'->>'systemid')::integer and (p_sysorgid is null or p_sysorgid=t1.sysorgid) and (p_sysorgname is null or position(p_sysorgname in t1.sysorgname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_orgtype is null or p_orgtype=t1.orgtype);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.isused,t1.upid,t1.isused,t1.sysorgid,t1.sysorgname,t1.description,t1.orgtype,t2.orgtypename,t1.idpath,t1.idlevel,tt.sysorgname upname from sysinfo.sysorg t1 left join sysinfo.sysorg tt on t1.upid=tt.sysorgid  left join sysinfo.orgtype t2 on t2.orgtypeid=t1.orgtype where (p_isused is null or t1.isused=p_isused) and (p_upid is null or p_upid=t1.upid) and (p_idpath is null or position(p_idpath in t1.idpath)>0)  and (p_isused is null or p_isused=t1.isused) and t1.systemid=(v_check->'info'->>'systemid')::integer and (p_sysorgid is null or p_sysorgid=t1.sysorgid) and (p_sysorgname is null or position(p_sysorgname in t1.sysorgname )>0) and (p_description is null or position(p_description in t1.description )>0) and (p_orgtype is null or p_orgtype=t1.orgtype) order by t1.idpath limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.sysorg_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) OWNER TO gm;

--
-- Name: FUNCTION sysorg_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysorg_query(p_token character varying, p_rows integer, p_page integer, p_upid integer, p_idpath character varying, p_isused integer, p_sysorgid integer, p_sysorgname character varying, p_description character varying, p_orgtype integer) IS '查询部门
upid:上级id
idpath:包含节点
isused:是否使用
sysorgid:部门id
sysorgname:部门名称
description:说明
orgtype:部门类型id
返回:{"total":总记录数,"rows":[{"upid":上级id,"isused":显示,"sysorgid":部门id,"sysorgname":部门名称,"description":说明,"orgtype":部门类型id,"orgtypename":部门类型名}]}';


--
-- Name: sysorg_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.sysorg_undel(p_token character varying, p_sysorgid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c integer;v_check json;
begin
  v_check:=gm.check_login(p_token,118);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_sysorgid=100 then return gm.returnjson(100015);end if; --顶级
  select count(*) into v_c from sysinfo.sysorg where sysorgid=p_sysorgid;
  if v_c=0 then return gm.returnjson(100018); end if;--部门信息出错！
  select count(*) into v_c from sysinfo.sysorg where upid=p_sysorgid and isused<>0;
  if v_c>0 then return gm.returnjson(100016);end if;--有下级类型不能删除
  update sysinfo.sysorg set isused=1 where sysorgid=p_sysorgid;
  with recursive tree as
  ( select sysorgid,upid,1 as idlevel,sysorgid::varchar as idpath,t1.systemid from sysinfo.sysorg t1 where coalesce(upid,0)=0
  union
  select t1.sysorgid,t1.upid,t2.idlevel+1,t2.idpath||'.'||t1.sysorgid,t1.systemid from sysinfo.sysorg t1 inner join tree t2 on t2.sysorgid=t1.upid and t2.systemid=t1.systemid where isused<>0) 
  update sysinfo.sysorg t set idpath=t1.idpath,idlevel=t1.idlevel,idcount=coalesce(cc,0) from tree t1 left join (select upid,count(*) cc,systemid from sysinfo.sysorg where isused<>0 group by upid,systemid) t2 on t1.sysorgid=t2.upid
  where t.sysorgid=t1.sysorgid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.sysorg_undel(p_token character varying, p_sysorgid integer) OWNER TO gm;

--
-- Name: FUNCTION sysorg_undel(p_token character varying, p_sysorgid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.sysorg_undel(p_token character varying, p_sysorgid integer) IS '恢复部门
sysorgid:id';


--
-- Name: systeminfo_add(character varying, integer, character varying, smallint, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.systeminfo_add(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,101);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_prikey is null then return gm.returnjson(100012); end if;--子系统信息出错！
  if p_systemid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.systeminfo_systemid_seq');
      select count(*) into v_c from sysinfo.systeminfo where systemid=v_id;
    end loop;
  else
    select count(*) into v_c from sysinfo.systeminfo where systemid=p_systemid;
    if v_c>0 then return gm.returnjson(100012); end if;--子系统信息出错！
    v_id:=p_systemid;
  end if;
  insert into sysinfo.systeminfo( systemid,systemname,isused,algorithm,prikey,loginname,createoperator,createtime) values(v_id,p_systemname,1,p_algorithm,p_prikey,p_loginname ,(v_check->'info'->>'operatorid')::integer,now());
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.systeminfo_add(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) OWNER TO gm;

--
-- Name: FUNCTION systeminfo_add(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.systeminfo_add(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) IS '新增子系统
systemid:系统编号
systemname:系统名
algorithm:加密函数1aes128,2aes256
prikey:密钥
loginname:系统登录名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: systeminfo_del(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.systeminfo_del(p_token character varying, p_systemid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,101);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.systeminfo where systemid=p_systemid;
  if v_c=0 then return gm.returnjson(100012); end if;--子系统信息出错！
  update sysinfo.systeminfo set isused=0,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where systemid=p_systemid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.systeminfo_del(p_token character varying, p_systemid integer) OWNER TO gm;

--
-- Name: FUNCTION systeminfo_del(p_token character varying, p_systemid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.systeminfo_del(p_token character varying, p_systemid integer) IS '删除子系统
systemid:id';


--
-- Name: systeminfo_edit(character varying, integer, character varying, smallint, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.systeminfo_edit(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,101);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_prikey is null then return gm.returnjson(100012); end if;--子系统信息出错！
  v_id:=p_systemid;
  select count(*) into v_c from sysinfo.systeminfo where systemid=p_systemid;
  if v_c=0 then return gm.returnjson(100012); end if;--子系统信息出错！
  update sysinfo.systeminfo set systemname=p_systemname,algorithm=p_algorithm,prikey=p_prikey,loginname=p_loginname ,updateoperator=(v_check->'info'->>'operatorid')::integer,updatetime=now() where systemid=p_systemid;
  return gm.returnjson(0);
end;
$$;


ALTER FUNCTION sysinfo.systeminfo_edit(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) OWNER TO gm;

--
-- Name: FUNCTION systeminfo_edit(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.systeminfo_edit(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) IS '修改子系统
systemid:系统编号
systemname:系统名
algorithm:加密函数1aes128,2aes256
prikey:密钥
loginname:系统登录名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: systeminfo_merge(character varying, integer, character varying, smallint, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.systeminfo_merge(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_id int;v_check json;
begin
  v_check:=gm.check_login(p_token,101);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_prikey is null then return gm.returnjson(100012); end if;--子系统信息出错！
  if p_systemid is null then 
    v_c:=1;loop exit when (v_c=0);
      v_id :=nextval('sysinfo.systeminfo_systemid_seq');
      select count(*) into v_c from sysinfo.systeminfo where systemid=v_id;
    end loop;
  else
    v_id:=p_systemid;
  end if;
  with "te" as (update sysinfo.systeminfo set systemname=p_systemname,algorithm=p_algorithm,prikey=p_prikey,loginname=p_loginname ,updateoperator=(v_check->'info'->>'operatorid')::integer,updatetime=now() where systemid=v_id returning *)
  insert into sysinfo.systeminfo ( systemid,systemname,isused,algorithm,prikey,loginname,createoperator,createtime) select v_id,p_systemname,1,p_algorithm,p_prikey,p_loginname ,(v_check->'info'->>'operatorid')::integer,now() where (select count(*) from te) = 0;
  return gm.returnjson(0,json_build_object('id',v_id));
end; 
$$;


ALTER FUNCTION sysinfo.systeminfo_merge(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) OWNER TO gm;

--
-- Name: FUNCTION systeminfo_merge(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.systeminfo_merge(p_token character varying, p_systemid integer, p_systemname character varying, p_algorithm smallint, p_prikey character varying, p_loginname character varying) IS '合并子系统
systemid:系统编号
systemname:系统名
algorithm:加密函数1aes128,2aes256
prikey:密钥
loginname:系统登录名
返回：{"errorcode":0,"message":"执行成功！","info":id}';


--
-- Name: systeminfo_query(character varying, integer, integer, integer, character varying, integer, smallint, character varying, character varying); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.systeminfo_query(p_token character varying, p_rows integer, p_page integer, p_systemid integer, p_systemname character varying, p_isused integer, p_algorithm smallint, p_prikey character varying, p_loginname character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_return json;v_check json;
begin
  v_check:=gm.check_login(p_token,101);
  if v_check->>'errorcode'<>'0' then return v_check; end if;

  if p_rows is null then
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.systemid,ts.systemname,t1.systemname,t1.isused,t1.algorithm,t1.prikey,t1.createoperator,tcreateoperator.operatorname createoperatorname,t1.createtime::varchar,t1.updateoperator,tupdateoperator.operatorname updateoperatorname,t1.updatetime::varchar,t1.deloperator,tdeloperator.operatorname deloperatorname,t1.deltime::varchar,t1.loginname from sysinfo.systeminfo t1 left join sysinfo.systeminfo ts on t1.systemid=ts.systemid left join sysinfo.operinfo tcreateoperator on t1.createoperator=tcreateoperator.operatorid left join sysinfo.operinfo tupdateoperator on t1.updateoperator=tupdateoperator.operatorid left join sysinfo.operinfo tdeloperator on t1.deloperator=tdeloperator.operatorid where  (p_systemid is null or t1.systemid=p_systemid) and (p_systemname is null or position(p_systemname in t1.systemname )>0) and (p_isused is null or p_isused=t1.isused) and (p_algorithm is null or p_algorithm=t1.algorithm) and (p_prikey is null or position(p_prikey in t1.prikey )>0) and (p_loginname is null or position(p_loginname in t1.loginname )>0) order by t1.systemid) t;
    v_c:=coalesce(json_array_length(v_return),0);
  else
    select count(*) into v_c from sysinfo.systeminfo t1 where  (p_systemid is null or t1.systemid=p_systemid) and (p_systemname is null or position(p_systemname in t1.systemname )>0) and (p_isused is null or p_isused=t1.isused) and (p_algorithm is null or p_algorithm=t1.algorithm) and (p_prikey is null or position(p_prikey in t1.prikey )>0) and (p_loginname is null or position(p_loginname in t1.loginname )>0);
    select array_to_json(array_agg(row_to_json(t))) into v_return from (select  t1.systemid,ts.systemname,t1.systemname,t1.isused,t1.algorithm,t1.prikey,t1.createoperator,tcreateoperator.operatorname createoperatorname,t1.createtime::varchar,t1.updateoperator,tupdateoperator.operatorname updateoperatorname,t1.updatetime::varchar,t1.deloperator,tdeloperator.operatorname deloperatorname,t1.deltime::varchar,t1.loginname from sysinfo.systeminfo t1 left join sysinfo.systeminfo ts on t1.systemid=ts.systemid left join sysinfo.operinfo tcreateoperator on t1.createoperator=tcreateoperator.operatorid left join sysinfo.operinfo tupdateoperator on t1.updateoperator=tupdateoperator.operatorid left join sysinfo.operinfo tdeloperator on t1.deloperator=tdeloperator.operatorid where  (p_systemid is null or t1.systemid=p_systemid) and (p_systemname is null or position(p_systemname in t1.systemname )>0) and (p_isused is null or p_isused=t1.isused) and (p_algorithm is null or p_algorithm=t1.algorithm) and (p_prikey is null or position(p_prikey in t1.prikey )>0) and (p_loginname is null or position(p_loginname in t1.loginname )>0) order by t1.systemid limit greatest(p_rows,1) offset greatest(least((p_page-1)*p_rows,(v_c/p_rows-1+abs(v_c % p_rows))*p_rows),0) ) t;
  end if;
  return gm.returnjson(0,json_build_object('total',v_c,'rows',v_return));
end;
$$;


ALTER FUNCTION sysinfo.systeminfo_query(p_token character varying, p_rows integer, p_page integer, p_systemid integer, p_systemname character varying, p_isused integer, p_algorithm smallint, p_prikey character varying, p_loginname character varying) OWNER TO gm;

--
-- Name: FUNCTION systeminfo_query(p_token character varying, p_rows integer, p_page integer, p_systemid integer, p_systemname character varying, p_isused integer, p_algorithm smallint, p_prikey character varying, p_loginname character varying); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.systeminfo_query(p_token character varying, p_rows integer, p_page integer, p_systemid integer, p_systemname character varying, p_isused integer, p_algorithm smallint, p_prikey character varying, p_loginname character varying) IS '查询子系统
systemid:系统编号
systemname:系统名
isused:是否使用
algorithm:加密函数1aes128,2aes256
prikey:密钥
loginname:系统登录名
返回:{"total":总记录数,"rows":[{"systemid":系统id,"systemname":系统名称,"systemname":系统名,"isused":使用,"algorithm":加密函数1aes128,2aes256,"prikey":密钥,"createoperator":操作员编号,"createoperatorname":创建人员,"updateoperator":操作员编号,"updateoperatorname":修改人员,"deloperator":操作员编号,"deloperatorname":删除人员,"loginname":系统登录名}]}';


--
-- Name: systeminfo_undel(character varying, integer); Type: FUNCTION; Schema: sysinfo; Owner: gm
--

CREATE FUNCTION sysinfo.systeminfo_undel(p_token character varying, p_systemid integer) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_c int;v_check json;
begin
  v_check:=gm.check_login(p_token,102);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select count(*) into v_c from sysinfo.systeminfo where systemid=p_systemid;
  if v_c=0 then return gm.returnjson(100012); end if;--子系统信息出错！
  update sysinfo.systeminfo set isused=1,deloperator=(v_check->'info'->>'operatorid')::integer,deltime=now() where systemid=p_systemid;
  return gm.returnjson(0); 
end;
$$;


ALTER FUNCTION sysinfo.systeminfo_undel(p_token character varying, p_systemid integer) OWNER TO gm;

--
-- Name: FUNCTION systeminfo_undel(p_token character varying, p_systemid integer); Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON FUNCTION sysinfo.systeminfo_undel(p_token character varying, p_systemid integer) IS '恢复子系统
systemid:id';


--
-- Name: nginx; Type: VIEW; Schema: gm; Owner: gm
--

CREATE VIEW gm.nginx AS
 SELECT string_agg(((('$'::text || (sort1)::text) || '::'::text) || (typname)::text), ','::text ORDER BY sort1) AS params,
    func,
    proargnames,
    pronargs
   FROM ( SELECT row_number() OVER (PARTITION BY t_1.func ORDER BY t_1.sort) AS sort1,
            t3.typname,
            t_1.func,
            t_1.pronargs,
            t_1.proargnames
           FROM (( SELECT row_number() OVER () AS sort,
                    t_2.func,
                    t_2.pronargs,
                    t_2.proargnames,
                    t_2.proargtypes,
                    t_2.aa
                   FROM ( SELECT ((((' "'::text || (t2.nspname)::text) || '"."'::text) || (t1.proname)::text) || '"'::text) AS func,
                            t1.pronargs,
                            (substr(((t1.proargnames)::character varying)::text, 2, (char_length(((t1.proargnames)::character varying)::text) - 2)) || ','::text) AS proargnames,
                            t1.proargtypes,
                            regexp_split_to_table(((t1.proargtypes)::character varying)::text, ' '::text) AS aa
                           FROM (pg_proc t1
                             LEFT JOIN pg_namespace t2 ON ((t1.pronamespace = t2.oid)))
                          WHERE (t2.nspname = 'sysinfo'::name)) t_2) t_1
             LEFT JOIN pg_type t3 ON ((t_1.aa = ((t3.oid)::character varying)::text)))) t
  GROUP BY func, pronargs, proargnames
  ORDER BY (func)::bytea;


ALTER VIEW gm.nginx OWNER TO gm;

--
-- Name: sysoperorg_sysorgid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sysoperorg_sysorgid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sysoperorg_sysorgid_seq OWNER TO postgres;

--
-- Name: sysorg_sysorgid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.sysorg_sysorgid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.sysorg_sysorgid_seq OWNER TO postgres;

--
-- Name: systeminfo_systemid_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.systeminfo_systemid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.systeminfo_systemid_seq OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: actions; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.actions (
    isused smallint,
    actionid integer NOT NULL,
    actionname character varying(255),
    description character varying(255),
    params character varying(255),
    actionurl character varying(255),
    code character varying(255)
);


ALTER TABLE sysinfo.actions OWNER TO gm;

--
-- Name: TABLE actions; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.actions IS '权限名';


--
-- Name: COLUMN actions.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.isused IS '使用';


--
-- Name: COLUMN actions.actionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.actionid IS '权限编号';


--
-- Name: COLUMN actions.actionname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.actionname IS '权限名';


--
-- Name: COLUMN actions.description; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.description IS '说明';


--
-- Name: COLUMN actions.params; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.params IS '参数';


--
-- Name: COLUMN actions.actionurl; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.actionurl IS '权限路径';


--
-- Name: COLUMN actions.code; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.actions.code IS '编码';


--
-- Name: appparams; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.appparams (
    appid integer NOT NULL,
    params character varying(255),
    accesstoken character varying(255),
    isused smallint,
    tokentime timestamp(6) without time zone,
    typeid integer
);


ALTER TABLE sysinfo.appparams OWNER TO gm;

--
-- Name: COLUMN appparams.appid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.appparams.appid IS 'appid';


--
-- Name: COLUMN appparams.params; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.appparams.params IS '参数';


--
-- Name: COLUMN appparams.accesstoken; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.appparams.accesstoken IS 'token';


--
-- Name: COLUMN appparams.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.appparams.isused IS '是否使用';


--
-- Name: COLUMN appparams.tokentime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.appparams.tokentime IS '生效时间';


--
-- Name: COLUMN appparams.typeid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.appparams.typeid IS '类型1:微信';


--
-- Name: city; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE public.city (
    pid integer NOT NULL,
    cid integer NOT NULL,
    city character varying(255),
    isused smallint
);


ALTER TABLE public.city OWNER TO gm;

--
-- Name: TABLE city; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE public.city IS '市';


--
-- Name: COLUMN city.pid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.city.pid IS '省id';


--
-- Name: COLUMN city.cid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.city.cid IS '市id';


--
-- Name: COLUMN city.city; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.city.city IS '市名';


--
-- Name: COLUMN city.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.city.isused IS '是否使用';


--
-- Name: city_cid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE public.city_cid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.city_cid_seq OWNER TO gm;

--
-- Name: errorcode; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.errorcode (
    message character varying(255),
    errorcode integer NOT NULL,
    primekey character varying(255),
    isused smallint,
    schema character varying(255)
);


ALTER TABLE sysinfo.errorcode OWNER TO gm;

--
-- Name: TABLE errorcode; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.errorcode IS '出错代码';


--
-- Name: COLUMN errorcode.message; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.errorcode.message IS '信息';


--
-- Name: COLUMN errorcode.errorcode; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.errorcode.errorcode IS '出错代码';


--
-- Name: COLUMN errorcode.primekey; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.errorcode.primekey IS '主键';


--
-- Name: COLUMN errorcode.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.errorcode.isused IS '使用';


--
-- Name: COLUMN errorcode.schema; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.errorcode.schema IS '模式';


--
-- Name: funchtml; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.funchtml (
    htmlid integer NOT NULL,
    html1 text,
    html2 text
);


ALTER TABLE sysinfo.funchtml OWNER TO gm;

-- sysinfo.loginlog 已并入 public.loginlog（2026-11-13 日志归拢），此处不再建表；
-- 登录日志统一存 public.loginlog（结构同原表：logid/logintime/operatorno/ip/accounts/systemid，无口令列），
-- logid 由 public.loginlog_logid_seq 生成（原 sysinfo 序列值平移 +100000，审计/请求日志同理避让）。

--
-- Name: operaccounts; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.operaccounts (
    operatorid integer,
    accounts character varying(255),
    appid integer,
    typeid integer,
    isused smallint,
    unionid character varying(255)
);


ALTER TABLE sysinfo.operaccounts OWNER TO gm;

--
-- Name: COLUMN operaccounts.operatorid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operaccounts.operatorid IS '操作员id';


--
-- Name: COLUMN operaccounts.accounts; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operaccounts.accounts IS '登录账号';


--
-- Name: COLUMN operaccounts.appid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operaccounts.appid IS 'appid';


--
-- Name: COLUMN operaccounts.typeid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operaccounts.typeid IS '101:微信';


--
-- Name: COLUMN operaccounts.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operaccounts.isused IS '是否使用';


--
-- Name: COLUMN operaccounts.unionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operaccounts.unionid IS '统一id';


--
-- Name: operinfo; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.operinfo (
    operatorid bigint NOT NULL,
    operatorname character varying(255),
    sex smallint,
    phone character varying(255),
    accounts character varying(255),
    pass character varying(255),
    tokenkey character varying(2000),
    tokentime timestamp(6) without time zone,
    memo character varying(255),
    isused smallint,
    mycode character varying(8),
    upcode character varying(8),
    headimgurl text,
    nickname character varying(255),
    birthday timestamp(6) without time zone,
    tokentype smallint DEFAULT 1,
    tokeninterval integer DEFAULT 180,
    createoperator integer,
    createtime timestamp(6) without time zone,
    updateoperator integer,
    updatetime timestamp(6) without time zone,
    deloperator integer,
    deltime timestamp(6) without time zone
);


ALTER TABLE sysinfo.operinfo OWNER TO gm;

--
-- Name: TABLE operinfo; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.operinfo IS '员工';


--
-- Name: COLUMN operinfo.operatorid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.operatorid IS '员工编号';


--
-- Name: COLUMN operinfo.operatorname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.operatorname IS '员工姓名';


--
-- Name: COLUMN operinfo.sex; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.sex IS '性别';


--
-- Name: COLUMN operinfo.phone; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.phone IS '电话';


--
-- Name: COLUMN operinfo.accounts; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.accounts IS '帐号';


--
-- Name: COLUMN operinfo.pass; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.pass IS '密码';


--
-- Name: COLUMN operinfo.tokenkey; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.tokenkey IS '令牌';


--
-- Name: COLUMN operinfo.tokentime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.tokentime IS '令牌时间';


--
-- Name: COLUMN operinfo.memo; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.memo IS '备注';


--
-- Name: COLUMN operinfo.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.isused IS '使用';


--
-- Name: COLUMN operinfo.mycode; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.mycode IS '推广码';


--
-- Name: COLUMN operinfo.upcode; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.upcode IS '推广上级id';


--
-- Name: COLUMN operinfo.headimgurl; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.headimgurl IS '头像url';


--
-- Name: COLUMN operinfo.nickname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.nickname IS '昵称';


--
-- Name: COLUMN operinfo.birthday; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.birthday IS '生日';


--
-- Name: COLUMN operinfo.tokentype; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.tokentype IS '令牌类型1默认单人登录2多人登录';


--
-- Name: COLUMN operinfo.tokeninterval; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.tokeninterval IS '令牌时长默认180分钟';


--
-- Name: COLUMN operinfo.createoperator; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.createoperator IS '创建人员';


--
-- Name: COLUMN operinfo.createtime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.createtime IS '创建时间';


--
-- Name: COLUMN operinfo.updateoperator; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.updateoperator IS '修改人员';


--
-- Name: COLUMN operinfo.updatetime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.updatetime IS '修改时间';


--
-- Name: COLUMN operinfo.deloperator; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.deloperator IS '删除人员';


--
-- Name: COLUMN operinfo.deltime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operinfo.deltime IS '删除时间';


--
-- Name: operinfo_operatorid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.operinfo_operatorid_seq
    START WITH 100000
    INCREMENT BY 1
    MINVALUE 100000
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.operinfo_operatorid_seq OWNER TO gm;

--
-- Name: operpermission_permissionid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.operpermission_permissionid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1
    CYCLE;


ALTER SEQUENCE sysinfo.operpermission_permissionid_seq OWNER TO gm;

--
-- Name: operpermission; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.operpermission (
    operatorid bigint NOT NULL,
    permissiontype smallint NOT NULL,
    ifpermission smallint,
    permissionorder integer,
    params character varying(255),
    sysactionid integer NOT NULL,
    permissionid integer DEFAULT nextval('sysinfo.operpermission_permissionid_seq'::regclass) NOT NULL
);


ALTER TABLE sysinfo.operpermission OWNER TO gm;

--
-- Name: TABLE operpermission; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.operpermission IS '员工权限';


--
-- Name: COLUMN operpermission.operatorid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.operatorid IS '员工编号';


--
-- Name: COLUMN operpermission.permissiontype; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.permissiontype IS '权限类型1权限2角色';


--
-- Name: COLUMN operpermission.ifpermission; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.ifpermission IS '允许';


--
-- Name: COLUMN operpermission.permissionorder; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.permissionorder IS '权限级别';


--
-- Name: COLUMN operpermission.params; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.params IS '参数';


--
-- Name: COLUMN operpermission.sysactionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.sysactionid IS '权限编号';


--
-- Name: COLUMN operpermission.permissionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.operpermission.permissionid IS '操作员权限编号';


--
-- Name: orgtype; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.orgtype (
    isused smallint,
    orgtypeid integer NOT NULL,
    orgtypename character varying(255),
    description character varying(255),
    systemid integer
);


ALTER TABLE sysinfo.orgtype OWNER TO gm;

--
-- Name: TABLE orgtype; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.orgtype IS '部门类型';


--
-- Name: COLUMN orgtype.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.orgtype.isused IS '显示';


--
-- Name: COLUMN orgtype.orgtypeid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.orgtype.orgtypeid IS '部门类型编号';


--
-- Name: COLUMN orgtype.orgtypename; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.orgtype.orgtypename IS '部门类型名';


--
-- Name: COLUMN orgtype.description; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.orgtype.description IS '说明';


--
-- Name: COLUMN orgtype.systemid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.orgtype.systemid IS '系统id';


--
-- Name: orgtype_orgtypeid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.orgtype_orgtypeid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.orgtype_orgtypeid_seq OWNER TO gm;

--
-- Name: province; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE public.province (
    pid integer NOT NULL,
    province character varying(255),
    isused smallint
);


ALTER TABLE public.province OWNER TO gm;

--
-- Name: TABLE province; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE public.province IS '省';


--
-- Name: COLUMN province.pid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.province.pid IS '省id';


--
-- Name: COLUMN province.province; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.province.province IS '省名';


--
-- Name: COLUMN province.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN public.province.isused IS '是否使用';


--
-- Name: province_pid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE public.province_pid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.province_pid_seq OWNER TO gm;

--
-- Name: roleinfo; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.roleinfo (
    roleinfoid integer NOT NULL,
    roleinfoname character varying(255),
    description character varying(255),
    isused smallint,
    systemid integer
);


ALTER TABLE sysinfo.roleinfo OWNER TO gm;

--
-- Name: TABLE roleinfo; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.roleinfo IS '角色';


--
-- Name: COLUMN roleinfo.roleinfoid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.roleinfo.roleinfoid IS '角色编号';


--
-- Name: COLUMN roleinfo.roleinfoname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.roleinfo.roleinfoname IS '角色名';


--
-- Name: COLUMN roleinfo.description; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.roleinfo.description IS '描述';


--
-- Name: COLUMN roleinfo.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.roleinfo.isused IS '显示';


--
-- Name: COLUMN roleinfo.systemid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.roleinfo.systemid IS '系统id';


--
-- Name: roleinfo_roleinfoid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.roleinfo_roleinfoid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.roleinfo_roleinfoid_seq OWNER TO gm;

--
-- Name: rolepermission; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.rolepermission (
    roleinfoid integer,
    permissionid integer NOT NULL,
    permissiontype smallint,
    ifpermission smallint,
    permissionorder integer,
    params character varying(255),
    sysactionid integer
);


ALTER TABLE sysinfo.rolepermission OWNER TO gm;

--
-- Name: TABLE rolepermission; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.rolepermission IS '角色权限';


--
-- Name: COLUMN rolepermission.roleinfoid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.roleinfoid IS '角色编号';


--
-- Name: COLUMN rolepermission.permissionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.permissionid IS '角色权限编号';


--
-- Name: COLUMN rolepermission.permissiontype; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.permissiontype IS '权限类型';


--
-- Name: COLUMN rolepermission.ifpermission; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.ifpermission IS '允许';


--
-- Name: COLUMN rolepermission.permissionorder; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.permissionorder IS '权限级别';


--
-- Name: COLUMN rolepermission.params; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.params IS '参数';


--
-- Name: COLUMN rolepermission.sysactionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.rolepermission.sysactionid IS '权限编号';


--
-- Name: rolepermission_permissionid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.rolepermission_permissionid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.rolepermission_permissionid_seq OWNER TO gm;

-- sysinfo.serverlog_logid_seq 已随 sysinfo.serverlog 并入 public.serverlog（2026-11-13 日志归拢），
-- logid 统一由 public.serverlog_logid_seq 生成（审计行 +100000 偏移），此处不再建序列。

-- sysinfo.serverlog 已并入 public.serverlog（2026-11-13 日志归拢），此处不再建表；
-- 保留空段位以便 dump 结构对照（真实建表见文件尾 public.serverlog 段）。

-- (sysinfo.serverlog 表/列注释已随表并入 public.serverlog，2026-11-13 日志归拢)


--
-- Name: sysaction; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.sysaction (
    isused smallint,
    idpath character varying(255),
    idlevel integer,
    idcount integer,
    upid integer,
    params character varying(255),
    systemid integer,
    sysactionid integer NOT NULL,
    sysactionname character varying(255),
    actionid integer,
    isdefault smallint
);


ALTER TABLE sysinfo.sysaction OWNER TO gm;

--
-- Name: TABLE sysaction; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.sysaction IS '系统权限';


--
-- Name: COLUMN sysaction.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.isused IS '使用';


--
-- Name: COLUMN sysaction.idpath; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.idpath IS ' ';


--
-- Name: COLUMN sysaction.idlevel; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.idlevel IS ' ';


--
-- Name: COLUMN sysaction.idcount; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.idcount IS ' ';


--
-- Name: COLUMN sysaction.upid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.upid IS '上级权限';


--
-- Name: COLUMN sysaction.params; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.params IS '参数';


--
-- Name: COLUMN sysaction.systemid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.systemid IS '系统编号';


--
-- Name: COLUMN sysaction.sysactionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.sysactionid IS '系统权限编号';


--
-- Name: COLUMN sysaction.sysactionname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.sysactionname IS '权限名';


--
-- Name: COLUMN sysaction.actionid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.actionid IS '权限编号';


--
-- Name: COLUMN sysaction.isdefault; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysaction.isdefault IS '是否默认';


--
-- Name: sysaction_sysactionid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.sysaction_sysactionid_seq
    START WITH 100000000
    INCREMENT BY 1
    MINVALUE 100000000
    NO MAXVALUE
    CACHE 1
    CYCLE;


ALTER SEQUENCE sysinfo.sysaction_sysactionid_seq OWNER TO gm;

--
-- Name: sysoper; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.sysoper (
    operatorid integer NOT NULL,
    systemid integer NOT NULL
);


ALTER TABLE sysinfo.sysoper OWNER TO gm;

--
-- Name: TABLE sysoper; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.sysoper IS '操作员所属系统';


--
-- Name: COLUMN sysoper.operatorid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysoper.operatorid IS '操作员id';


--
-- Name: COLUMN sysoper.systemid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysoper.systemid IS '系统id';


--
-- Name: sysoperorg; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.sysoperorg (
    operatorid integer NOT NULL,
    sysorgid integer NOT NULL
);


ALTER TABLE sysinfo.sysoperorg OWNER TO gm;

--
-- Name: TABLE sysoperorg; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.sysoperorg IS '操作员部门';


--
-- Name: COLUMN sysoperorg.operatorid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysoperorg.operatorid IS '操作员id';


--
-- Name: COLUMN sysoperorg.sysorgid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysoperorg.sysorgid IS '系统部门id';


--
-- Name: sysoperorg_sysorgid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.sysoperorg_sysorgid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.sysoperorg_sysorgid_seq OWNER TO gm;

--
-- Name: sysorg; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.sysorg (
    isused smallint,
    idpath character varying(255),
    idlevel integer,
    idcount integer,
    upid integer,
    systemid integer,
    sysorgid integer NOT NULL,
    sysorgname character varying(255),
    description character varying(255),
    orgtype integer
);


ALTER TABLE sysinfo.sysorg OWNER TO gm;

--
-- Name: TABLE sysorg; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.sysorg IS '部门';


--
-- Name: COLUMN sysorg.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.isused IS '显示';


--
-- Name: COLUMN sysorg.upid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.upid IS '上级部门';


--
-- Name: COLUMN sysorg.systemid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.systemid IS '系统id';


--
-- Name: COLUMN sysorg.sysorgid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.sysorgid IS '部门id';


--
-- Name: COLUMN sysorg.sysorgname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.sysorgname IS '部门名称';


--
-- Name: COLUMN sysorg.description; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.description IS '说明';


--
-- Name: COLUMN sysorg.orgtype; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.sysorg.orgtype IS '部门类型id';


--
-- Name: sysorg_sysorgid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.sysorg_sysorgid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.sysorg_sysorgid_seq OWNER TO gm;

--
-- Name: systeminfo; Type: TABLE; Schema: sysinfo; Owner: gm
--

CREATE TABLE sysinfo.systeminfo (
    systemid integer NOT NULL,
    systemname character varying(255),
    isused smallint,
    algorithm smallint,
    prikey character varying(16) NOT NULL,
    createoperator integer,
    createtime timestamp(6) without time zone,
    updateoperator integer,
    updatetime timestamp(6) without time zone,
    deloperator integer,
    deltime timestamp(6) without time zone,
    loginname character varying(255)
);


ALTER TABLE sysinfo.systeminfo OWNER TO gm;

--
-- Name: TABLE systeminfo; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON TABLE sysinfo.systeminfo IS '子系统';


--
-- Name: COLUMN systeminfo.systemid; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.systemid IS '系统编号';


--
-- Name: COLUMN systeminfo.systemname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.systemname IS '系统名';


--
-- Name: COLUMN systeminfo.isused; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.isused IS '使用';


--
-- Name: COLUMN systeminfo.algorithm; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.algorithm IS '加密函数1aes128,2aes256';


--
-- Name: COLUMN systeminfo.prikey; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.prikey IS '密钥';


--
-- Name: COLUMN systeminfo.createoperator; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.createoperator IS '创建人员';


--
-- Name: COLUMN systeminfo.createtime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.createtime IS '创建时间';


--
-- Name: COLUMN systeminfo.updateoperator; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.updateoperator IS '修改人员';


--
-- Name: COLUMN systeminfo.updatetime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.updatetime IS '修改时间';


--
-- Name: COLUMN systeminfo.deloperator; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.deloperator IS '删除人员';


--
-- Name: COLUMN systeminfo.deltime; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.deltime IS '删除时间';


--
-- Name: COLUMN systeminfo.loginname; Type: COMMENT; Schema: sysinfo; Owner: gm
--

COMMENT ON COLUMN sysinfo.systeminfo.loginname IS '系统登录名';


--
-- Name: systeminfo_systemid_seq; Type: SEQUENCE; Schema: sysinfo; Owner: gm
--

CREATE SEQUENCE sysinfo.systeminfo_systemid_seq
    START WITH 100
    INCREMENT BY 1
    MINVALUE 100
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE sysinfo.systeminfo_systemid_seq OWNER TO gm;

--
-- Data for Name: actions; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.actions (isused, actionid, actionname, description, params, actionurl, code) FROM stdin;
1	150	登录	\N	\N	sysinfo/login	\N
1	160	微信登录	\N	101	sysinfo/loginaccount	\N
1	161	注册	\N	\N	sysinfo/reg	\N
1	171	微信注册	\N	101	sysinfo/regmicrochat	\N
1	128	合并员工所属系统	\N	\N	sysinfo/operinfo_merge	\N
1	129	删除员工所属系统	\N	\N	sysinfo/operinfo_del	\N
1	130	恢复员工所属系统	\N	\N	sysinfo/operinfo_undel	\N
1	131	查询员工所属系统	\N	\N	sysinfo/operinfo_query	\N
1	114	新增部门	\N	\N	sysinfo/sysorg_add	\N
1	146	合并员工部门	\N	\N	sysinfo/operinfoorg_merge	\N
1	147	删除员工部门	\N	\N	sysinfo/operinfoorg_del	\N
1	101	新增子系统	\N	Gao@12345	sysinfo/systeminfo_add	\N
1	102	修改子系统	\N	\N	sysinfo/systeminfo_edit	\N
1	103	合并子系统	\N	\N	sysinfo/systeminfo_merge	\N
1	104	删除子系统	\N	\N	sysinfo/systeminfo_del	\N
1	105	恢复子系统	\N	\N	sysinfo/systeminfo_undel	\N
1	106	查询子系统	\N	\N	sysinfo/systeminfo_query	\N
1	148	恢复员工部门	\N	\N	sysinfo/operinfoorg_undel	\N
1	149	查询员工部门	\N	\N	sysinfo/operinfo_query	\N
1	138	新增员工权限	\N	\N	sysinfo/operinfopermission_add	\N
1	139	修改员工权限	\N	\N	sysinfo/operinfopermission_edit	\N
1	140	合并员工权限	\N	\N	sysinfo/operinfopermission_merge	\N
1	141	删除员工权限	\N	\N	sysinfo/operinfopermission_del	\N
1	142	恢复员工权限	\N	\N	sysinfo/operinfopermission_undel	\N
1	143	查询员工权限	\N	\N	sysinfo/operinfo_query	\N
1	115	修改部门	\N	\N	sysinfo/sysorg_edit	\N
1	116	合并部门	\N	\N	sysinfo/sysorg_merge	\N
1	132	新增角色	\N	\N	sysinfo/roleinfo_add	\N
1	133	修改角色	\N	\N	sysinfo/roleinfo_edit	\N
1	134	合并角色	\N	\N	sysinfo/roleinfo_merge	\N
1	135	删除角色	\N	\N	sysinfo/roleinfo_del	\N
1	136	恢复角色	\N	\N	sysinfo/roleinfo_undel	\N
1	137	查询角色	\N	\N	sysinfo/roleinfo_query	\N
1	145	修改员工部门	\N	\N	sysinfo/operinfoorg_edit	\N
1	144	新增员工部门	\N	\N	sysinfo/operinfoorg_add	\N
1	117	删除部门	\N	\N	sysinfo/sysorg_del	\N
1	118	恢复部门	\N	\N	sysinfo/sysorg_undel	\N
1	119	查询部门	\N	\N	sysinfo/sysorg_query	\N
1	182	新增省	\N	\N	sysinfo/province_add	\N
1	183	修改省	\N	\N	sysinfo/province_edit	\N
1	184	合并省	\N	\N	sysinfo/province_merge	\N
1	185	删除省	\N	\N	sysinfo/province_del	\N
1	187	查询省	\N	\N	sysinfo/province_query	\N
1	107	查询系统日志	\N	\N	sysinfo/serverlog_query	\N
1	180	查询权限名	\N	\N	sysinfo/actions_query	\N
1	126	新增员工所属系统	\N	\N	sysinfo/operinfo_add	\N
1	127	修改员工所属系统	\N	\N	sysinfo/operinfo_edit	\N
1	181	查询出错代码	\N	\N	sysinfo/errorcode_query	\N
1	186	恢复省	\N	\N	sysinfo/province_undel	\N
1	188	新增市	\N	\N	sysinfo/city_add	\N
1	189	修改市	\N	\N	sysinfo/city_edit	\N
1	120	新增部门类型	\N	\N	sysinfo/orgtype_add	\N
1	121	修改部门类型	\N	\N	sysinfo/orgtype_edit	\N
1	122	合并部门类型	\N	\N	sysinfo/orgtype_merge	\N
1	123	删除部门类型	\N	\N	sysinfo/orgtype_del	\N
1	124	恢复部门类型	\N	\N	sysinfo/orgtype_undel	\N
1	125	查询部门类型	\N	\N	sysinfo/orgtype_query	\N
1	190	合并市	\N	\N	sysinfo/city_merge	\N
1	191	删除市	\N	\N	sysinfo/city_del	\N
1	192	恢复市	\N	\N	sysinfo/city_undel	\N
1	193	查询市	\N	\N	sysinfo/city_query	\N
1	108	新增系统权限	\N	\N	sysinfo/sysaction_add	\N
1	109	修改系统权限	\N	\N	sysinfo/sysaction_edit	\N
1	110	合并系统权限	\N	\N	sysinfo/sysaction_merge	\N
1	111	删除系统权限	\N	\N	sysinfo/sysaction_del	\N
1	112	恢复系统权限	\N	\N	sysinfo/sysaction_undel	\N
1	113	查询系统权限	\N	\N	sysinfo/sysaction_query	\N
\.


--
-- Data for Name: appparams; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.appparams (appid, params, accesstoken, isused, tokentime, typeid) FROM stdin;
100	appid=xxx&secret=xxx	xxx	1	2023-08-23 17:08:57.622318	101
\.


--
-- Data for Name: city; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY public.city (pid, cid, city, isused) FROM stdin;
11	1100	北京市	\N
13	1301	石家庄市	\N
44	4409	茂名市	\N
44	4412	肇庆市	\N
44	4413	惠州市	\N
44	4414	梅州市	\N
44	4415	汕尾市	\N
44	4416	河源市	\N
44	4417	阳江市	\N
44	4418	清远市	\N
44	4419	东莞市	\N
44	4420	中山市	\N
44	4451	潮州市	\N
44	4452	揭阳市	\N
44	4453	云浮市	\N
45	4501	南宁市	\N
45	4502	柳州市	\N
13	1308	承德市	\N
13	1309	沧州市	\N
13	1310	廊坊市	\N
13	1311	衡水市	\N
14	1401	太原市	\N
14	1402	大同市	\N
14	1403	阳泉市	\N
14	1404	长治市	\N
53	5307	丽江市	\N
14	1405	晋城市	\N
14	1406	朔州市	\N
14	1407	晋中市	\N
14	1408	运城市	\N
14	1409	忻州市	\N
14	1410	临汾市	\N
14	1411	吕梁市	\N
15	1501	呼和浩特市	\N
15	1502	包头市	\N
15	1503	乌海市	\N
15	1504	赤峰市	\N
15	1505	通辽市	\N
15	1506	鄂尔多斯市	\N
15	1507	呼伦贝尔市	\N
15	1508	巴彦淖尔市	\N
41	4111	漯河市	\N
15	1509	乌兰察布市	\N
15	1525	锡林郭勒盟	\N
61	6103	宝鸡市	\N
15	1529	阿拉善盟	\N
21	2101	沈阳市	\N
21	2102	大连市	\N
21	2103	鞍山市	\N
21	2104	抚顺市	\N
21	2105	本溪市	\N
21	2106	丹东市	\N
21	2107	锦州市	\N
21	2108	营口市	\N
21	2109	阜新市	\N
21	2110	辽阳市	\N
21	2111	盘锦市	\N
21	2112	铁岭市	\N
21	2113	朝阳市	\N
21	2114	葫芦岛市	\N
22	2201	长春市	\N
22	2202	吉林市	\N
22	2203	四平市	\N
22	2204	辽源市	\N
22	2205	通化市	\N
22	2206	白山市	\N
22	2207	松原市	\N
22	2208	白城市	\N
22	2224	延边朝鲜族自治州	\N
23	2301	哈尔滨市	\N
23	2302	齐齐哈尔市	\N
23	2305	双鸭山市	\N
23	2306	大庆市	\N
23	2308	佳木斯市	\N
23	2309	七台河市	\N
23	2310	牡丹江市	\N
23	2307	伊春市	\N
23	2311	黑河市	\N
23	2312	绥化市	\N
23	2327	大兴安岭地区	\N
32	3202	无锡市	\N
32	3203	徐州市	\N
32	3204	常州市	\N
32	3205	苏州市	\N
32	3206	南通市	\N
32	3207	连云港市	\N
32	3208	淮安市	\N
32	3209	盐城市	\N
32	3210	扬州市	\N
32	3212	泰州市	\N
32	3213	宿迁市	\N
33	3301	杭州市	\N
33	3302	宁波市	\N
33	3303	温州市	\N
33	3304	嘉兴市	\N
33	3305	湖州市	\N
33	3306	绍兴市	\N
33	3307	金华市	\N
33	3308	衢州市	\N
33	3309	舟山市	\N
33	3310	台州市	\N
33	3311	丽水市	\N
34	3401	合肥市	\N
34	3402	芜湖市	\N
34	3403	蚌埠市	\N
34	3404	淮南市	\N
34	3405	马鞍山市	\N
34	3406	淮北市	\N
34	3407	铜陵市	\N
52	5203	遵义市	\N
34	3410	黄山市	\N
34	3411	滁州市	\N
34	3412	阜阳市	\N
34	3413	宿州市	\N
34	3415	六安市	\N
34	3416	亳州市	\N
34	3417	池州市	\N
34	3418	宣城市	\N
41	4107	新乡市	\N
35	3501	福州市	\N
35	3502	厦门市	\N
35	3503	莆田市	\N
35	3504	三明市	\N
35	3505	泉州市	\N
35	3506	漳州市	\N
35	3507	南平市	\N
35	3508	龙岩市	\N
35	3509	宁德市	\N
36	3601	南昌市	\N
36	3602	景德镇市	\N
36	3603	萍乡市	\N
36	3604	九江市	\N
36	3605	新余市	\N
36	3606	鹰潭市	\N
36	3607	赣州市	\N
36	3608	吉安市	\N
36	3609	宜春市	\N
36	3610	抚州市	\N
36	3611	上饶市	\N
37	3701	济南市	\N
37	3702	青岛市	\N
37	3703	淄博市	\N
37	3704	枣庄市	\N
37	3705	东营市	\N
37	3706	烟台市	\N
37	3707	潍坊市	\N
37	3708	济宁市	\N
37	3709	泰安市	\N
37	3710	威海市	\N
37	3711	日照市	\N
37	3712	莱芜市	\N
37	3713	临沂市	\N
37	3714	德州市	\N
37	3715	聊城市	\N
37	3716	滨州市	\N
37	3717	菏泽市	\N
41	4101	郑州市	\N
41	4102	开封市	\N
41	4103	洛阳市	\N
41	4104	平顶山市	\N
41	4105	安阳市	\N
41	4106	鹤壁市	\N
41	4108	焦作市	\N
41	4109	濮阳市	\N
41	4110	许昌市	\N
41	4112	三门峡市	\N
41	4113	南阳市	\N
41	4114	商丘市	\N
41	4115	信阳市	\N
41	4116	周口市	\N
41	4117	驻马店市	\N
42	4201	武汉市	\N
42	4202	黄石市	\N
42	4203	十堰市	\N
42	4205	宜昌市	\N
42	4206	襄阳市	\N
42	4207	鄂州市	\N
42	4208	荆门市	\N
42	4209	孝感市	\N
42	4210	荆州市	\N
42	4211	黄冈市	\N
42	4213	随州市	\N
42	4228	恩施土家族苗族自治州	\N
43	4302	株洲市	\N
43	4303	湘潭市	\N
43	4304	衡阳市	\N
43	4305	邵阳市	\N
43	4306	岳阳市	\N
43	4307	常德市	\N
43	4308	张家界市	\N
43	4309	益阳市	\N
43	4310	郴州市	\N
43	4311	永州市	\N
43	4312	怀化市	\N
43	4313	娄底市	\N
43	4331	湘西土家族苗族自治州	\N
62	6211	定西市	\N
44	4401	广州市	\N
44	4402	韶关市	\N
44	4403	深圳市	\N
44	4404	珠海市	\N
44	4405	汕头市	\N
44	4407	江门市	\N
45	4503	桂林市	\N
45	4504	梧州市	\N
45	4505	北海市	\N
45	4506	防城港市	\N
45	4507	钦州市	\N
45	4508	贵港市	\N
45	4509	玉林市	\N
45	4510	百色市	\N
45	4511	贺州市	\N
45	4512	河池市	\N
46	4603	三沙市	\N
46	4604	儋州市	\N
51	5101	成都市	\N
51	5103	自贡市	\N
51	5104	攀枝花市	\N
51	5105	泸州市	\N
51	5106	德阳市	\N
51	5107	绵阳市	\N
51	5108	广元市	\N
51	5109	遂宁市	\N
51	5110	内江市	\N
51	5111	乐山市	\N
51	5113	南充市	\N
51	5114	眉山市	\N
51	5115	宜宾市	\N
51	5116	广安市	\N
51	5118	雅安市	\N
51	5119	巴中市	\N
45	4513	来宾市	\N
45	4514	崇左市	\N
46	4601	海口市	\N
46	4602	三亚市	\N
51	5120	资阳市	\N
51	5132	阿坝藏族羌族自治州	\N
51	5133	甘孜藏族自治州	\N
51	5134	凉山彝族自治州	\N
52	5201	贵阳市	\N
52	5202	六盘水市	\N
52	5204	安顺市	\N
52	5205	毕节市	\N
52	5206	铜仁市	\N
52	5223	黔西南布依族苗族自治州	\N
52	5227	黔南布依族苗族自治州	\N
53	5301	昆明市	\N
53	5303	曲靖市	\N
53	5304	玉溪市	\N
53	5305	保山市	\N
53	5306	昭通市	\N
53	5308	普洱市	\N
53	5323	楚雄彝族自治州	\N
53	5325	红河哈尼族彝族自治州	\N
53	5326	文山壮族苗族自治州	\N
53	5328	西双版纳傣族自治州	\N
53	5329	大理白族自治州	\N
53	5331	德宏傣族景颇族自治州	\N
53	5333	怒江傈僳族自治州	\N
53	5334	迪庆藏族自治州	\N
54	5401	拉萨市	\N
54	5402	日喀则市	\N
54	5403	昌都市	\N
54	5404	林芝市	\N
54	5405	山南市	\N
54	5424	那曲地区	\N
54	5425	阿里地区	\N
61	6101	西安市	\N
61	6102	铜川市	\N
61	6104	咸阳市	\N
44	4408	湛江市	\N
61	6105	渭南市	\N
61	6106	延安市	\N
61	6107	汉中市	\N
61	6108	榆林市	\N
61	6109	安康市	\N
61	6110	商洛市	\N
62	6201	兰州市	\N
62	6202	嘉峪关市	\N
62	6203	金昌市	\N
62	6204	白银市	\N
62	6205	天水市	\N
62	6206	武威市	\N
62	6207	张掖市	\N
62	6208	平凉市	\N
62	6209	酒泉市	\N
62	6210	庆阳市	\N
62	6212	陇南市	\N
62	6229	临夏回族自治州	\N
62	6230	甘南藏族自治州	\N
63	6301	西宁市	\N
63	6302	海东市	\N
63	6322	海北藏族自治州	\N
63	6323	黄南藏族自治州	\N
63	6325	海南藏族自治州	\N
63	6327	玉树藏族自治州	\N
63	6328	海西蒙古族藏族自治州	\N
64	6401	银川市	\N
64	6402	石嘴山市	\N
64	6403	吴忠市	\N
64	6404	固原市	\N
64	6405	中卫市	\N
65	6501	乌鲁木齐市	\N
65	6502	克拉玛依市	\N
65	6504	吐鲁番市	\N
65	6505	哈密市	\N
65	6523	昌吉回族自治州	\N
65	6527	博尔塔拉蒙古自治州	\N
65	6528	巴音郭楞蒙古自治州	\N
65	6529	阿克苏地区	\N
65	6530	克孜勒苏柯尔克孜自治州	\N
65	6532	和田地区	\N
65	6540	伊犁哈萨克自治州	\N
65	6542	塔城地区	\N
65	6543	阿勒泰地区	\N
71	7101	台北市	\N
71	7102	高雄市	\N
71	7103	台南市	\N
71	7104	台中市	\N
71	7106	南投县	\N
71	7107	基隆市	\N
71	7108	新竹市	\N
71	7109	嘉义市	\N
71	7111	新北市	\N
71	7112	宜兰县	\N
71	7113	新竹县	\N
71	7115	苗栗县	\N
71	7119	嘉义县	\N
71	7121	云林县	\N
71	7124	屏东县	\N
71	7125	台东县	\N
71	7126	花莲县	\N
71	7127	澎湖县	\N
81	8100	香港特别行政区	\N
82	8200	澳门特别行政区	\N
31	3100	上海市	\N
50	5000	重庆市	\N
13	1303	秦皇岛市	\N
13	1304	邯郸市	\N
13	1305	邢台市	\N
15	1522	兴安盟	\N
32	3201	南京市	\N
34	3408	安庆市	\N
51	5117	达州市	\N
71	7117	彰化县	\N
65	6531	喀什地区	\N
13	1306	保定市	\N
13	1307	张家口市	\N
23	2303	鸡西市	\N
32	3211	镇江市	\N
43	4301	长沙市	\N
44	4406	佛山市	\N
63	6326	果洛藏族自治州	\N
13	1302	唐山市	\N
12	1200	天津市	\N
23	2304	鹤岗市	\N
42	4212	咸宁市	\N
52	5226	黔东南苗族侗族自治州	\N
53	5309	临沧市	\N
71	7114	桃园市	\N
\.


--
-- Data for Name: errorcode; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.errorcode (message, errorcode, primekey, isused, schema) FROM stdin;
操作员信息出错！	100011	operatorid	1	sysinfo.operinfo
权限信息出错！	100013	sysactionid	1	sysinfo.sysaction
上级不存在！	100014	\N	1	\N
顶级不能删除！	100015	\N	1	\N
执行成功！	0	\N	1	\N
有下级不能删除！	100016	\N	1	\N
员工权限信息出错！	100025	permissionid	1	sysinfo.operpermission
部门信息出错！	100018	sysorgid	1	sysinfo.sysorg
部门类型信息出错！	100020	orgtypeid	1	sysinfo.orgtype
不能注册！	100026	\N	\N	\N
帐号密码错误	100001	\N	1	\N
注册码不正确！	100027	\N	\N	\N
登录错误超过5次，帐号锁定10分钟！	100002	\N	1	\N
登录错误超过10次，帐号锁定3小时！	100003	\N	1	\N
手机码不正确！	100028	\N	\N	\N
小程序已注册！	100029	\N	\N	\N
非法登录！	100005	\N	1	\N
登录已失效！	100006	\N	1	\N
账号不能重复！	100022	\N	\N	\N
角色信息出错！	100023	roleinfoid	1	sysinfo.roleinfo
无此权限	100008	\N	1	\N
帐号不能为空	100009	\N	1	\N
密码不能为空！	100010	\N	1	\N
操作员部门信息出错！	100019	sysorgid	1	sysinfo.sysoperorg
角色权限信息出错！	100024	permissionid	1	sysinfo.rolepermission
子系统信息出错！	100012	systemid	1	sysinfo.systeminfo
操作员所属系统信息出错！	100021	systemid	1	sysinfo.sysoper
不能循环定义！	100017	\N	1	\N
系统日志信息出错！	100030	logid	1	public.serverlog
权限名信息出错！	100031	actionid	1	sysinfo.actions
出错代码信息出错！	100032	errorcode	1	sysinfo.errorcode
省信息出错！	100033	pid	1	public.province
市信息出错！	100034	cid	1	public.city
\.


--
-- Data for Name: funchtml; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.funchtml (htmlid, html1, html2) FROM stdin;
\.


-- (sysinfo.loginlog 数据已迁入 public.loginlog，2026-11-13 日志归拢)


--
-- Data for Name: operaccounts; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.operaccounts (operatorid, accounts, appid, typeid, isused, unionid) FROM stdin;
\.


--
-- Data for Name: operinfo; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.operinfo (operatorid, operatorname, sex, phone, accounts, pass, tokenkey, tokentime, memo, isused, mycode, upcode, headimgurl, nickname, birthday, tokentype, tokeninterval, createoperator, createtime, updateoperator, updatetime, deloperator, deltime) FROM stdin;
100000	系统管理员	\N	\N	admin	$2a$08$yEUQoAcSFxz6dJW1fK1y6O/02rRrPo0OR9y4I2tgzzwlGMbQU9AiG	ww0EBwMCvBe6ua6Q7UN20sBqAZterO0e4EKICVYvgiFHGdi+2tX99gTg22IxpLrfYklW1KRg1mlDu4daK28lV3OusNz6isfZWIwjeL54B9N1gI6sw1C3OkKb3P9wSKg3tiA6MbtOxz4/zVWPhYEQNvkD1cEyvDaXeSz/eJS+jRPKIJI3OQ1UjEzcZdmOSTpg++dRhKPN5HP+bpNQk/C0HhPLTVIV/7FAERzUNukdmsrJEdUuQOhC0tVFW7l5j1sP/T7U9a5WB/p0zLjxfPhMvoGJTd5akXywv2JuosFI9zr11pj7J55HtI3j+KLo4b0pQwCcN09NeJSOW3QtDPZtOB4Epg+tpPuxASQs3ihxW744idMvmVi1R/kbWJelKJvkrgJ4w06cpbBkJazsIR0S0okR6W2BYduc24DjtQ==	2026-11-10 00:50:35.152138	\N	1	\N	\N	\N	\N	\N	1	180	\N	\N	\N	\N	\N	\N
\.


--
-- Data for Name: operpermission; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.operpermission (operatorid, permissiontype, ifpermission, permissionorder, params, sysactionid, permissionid) FROM stdin;
100000	1	\N	\N	\N	100	100
\.


--
-- Data for Name: orgtype; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.orgtype (isused, orgtypeid, orgtypename, description, systemid) FROM stdin;
1	100	部门	\N	100
\.


--
-- Data for Name: province; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY public.province (pid, province, isused) FROM stdin;
34	安徽省	\N
32	江苏省	\N
33	浙江省	\N
46	海南省	\N
64	宁夏回族自治区	\N
65	新疆维吾尔自治区	\N
45	广西壮族自治区	\N
62	甘肃省	\N
22	吉林省	\N
61	陕西省	\N
81	香港特别行政区	\N
14	山西省	\N
51	四川省	\N
23	黑龙江省	\N
21	辽宁省	\N
54	西藏自治区	\N
52	贵州省	\N
44	广东省	\N
53	云南省	\N
50	重庆市	\N
12	天津市	\N
15	内蒙古自治区	\N
31	上海市	\N
11	北京市	\N
43	湖南省	\N
63	青海省	\N
13	河北省	\N
41	河南省	\N
35	福建省	\N
37	山东省	\N
36	江西省	\N
71	台湾省	\N
82	澳门特别行政区	\N
42	湖北省	\N
\.


--
-- Data for Name: roleinfo; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.roleinfo (roleinfoid, roleinfoname, description, isused, systemid) FROM stdin;
\.


--
-- Data for Name: rolepermission; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.rolepermission (roleinfoid, permissionid, permissiontype, ifpermission, permissionorder, params, sysactionid) FROM stdin;
\.


-- (sysinfo.serverlog 数据已迁入 public.serverlog，2026-11-13 日志归拢)


--
-- Data for Name: sysaction; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.sysaction (isused, idpath, idlevel, idcount, upid, params, systemid, sysactionid, sysactionname, actionid, isdefault) FROM stdin;
1	100.9003	2	2	100	\N	100	9003	公共数据	\N	\N
1	100.9003.9112	3	6	9003	\N	100	9112	市	\N	\N
1	100.9003.9112.188	4	0	9112	\N	100	188	新增市	188	\N
1	100.9003.9112.189	4	0	9112	\N	100	189	修改市	189	\N
1	100.9003.9112.190	4	0	9112	\N	100	190	修改市	190	\N
1	100.9003.9112.191	4	0	9112	\N	100	191	删除市	191	\N
1	100.9002.9102.117	4	0	9102	\N	100	117	删除部门	117	\N
1	100.9002.9105.133	4	0	9105	\N	100	133	修改角色	133	\N
1	100.9002.9105.137	4	0	9105	\N	100	137	查询角色	137	\N
1	100.9002.9102.119	4	0	9102	\N	100	119	查询部门	119	\N
1	100.9002.9102.115	4	0	9102	\N	100	115	修改部门	115	\N
1	100.9001.9103.106	4	0	9103	\N	100	106	查询子系统	106	\N
1	100.9001.9100.111	4	0	9100	\N	100	111	删除系统权限	111	\N
1	100.9002.9106	3	6	9002	\N	100	9106	员工部门	\N	\N
1	100.9003.9111.187	4	0	9111	\N	100	187	查询省	187	\N
1	100.9001.9110.107	4	0	9110	\N	100	107	查询系统日志	107	\N
1	100.9002.9106.144	4	0	9106	\N	100	144	新增员工部门	144	\N
1	100.9002.9106.148	4	0	9106	\N	100	148	恢复员工部门	148	\N
1	100.9001.9104.126	4	0	9104	\N	100	126	新增员工所属系统	126	\N
1	100.9001.9100.112	4	0	9100	\N	100	112	恢复系统权限	112	\N
1	100.9002.9101	3	6	9002	\N	100	9101	部门类型	\N	\N
1	100.9002.9102.114	4	0	9102	\N	100	114	新增部门	114	\N
1	100.9002.9102	3	6	9002	\N	100	9102	部门	\N	\N
1	100.9001.9104	3	6	9001	\N	100	9104	员工所属系统	\N	\N
1	100.9002.9107	3	6	9002	\N	100	9107	员工权限	\N	\N
1	100.150	2	0	100	\N	100	150	登录	150	\N
1	100.9001.9100.108	4	0	9100	\N	100	108	新增系统权限	108	\N
1	100.9001.9100.113	4	0	9100	\N	100	113	查询系统权限	113	\N
1	100.9001.9103.105	4	0	9103	\N	100	105	恢复子系统	105	\N
1	100.171	2	0	100	101	100	171	微信注册	171	\N
1	100.9002.9101.120	4	0	9101	\N	100	120	新增部门类型	120	\N
1	100.9003.9111.182	4	0	9111	\N	100	182	新增省	182	\N
1	100.9001.9103	3	6	9001	\N	100	9103	子系统管理	\N	\N
1	100.9002.9105	3	6	9002	\N	100	9105	角色	\N	\N
1	100.9002.9105.134	4	0	9105	\N	100	134	合并角色	134	\N
1	100.9002.9107.138	4	0	9107	\N	100	138	新增员工权限	138	\N
1	100.9002.9106.146	4	0	9106	\N	100	146	合并员工部门	146	\N
1	100.9002.9106.145	4	0	9106	\N	100	145	修改员工部门	145	\N
1	100.9002.9106.147	4	0	9106	\N	100	147	删除员工部门	147	\N
1	100.9003.9111	3	6	9003	\N	100	9111	省	\N	\N
1	100.9003.9112.192	4	0	9112	\N	100	192	恢复市	192	\N
1	100.9003.9112.193	4	0	9112	\N	100	193	查询市	193	\N
1	100.9001.9104.127	4	0	9104	\N	100	127	修改员工所属系统	127	\N
1	100	1	7	0	\N	100	100	系统	\N	\N
1	100.9001	2	6	100	\N	100	9001	系统管理	\N	\N
1	100.9002	2	5	100	\N	100	9002	本系统管理	\N	\N
1	100.9002.9102.118	4	0	9102	\N	100	118	恢复部门	118	\N
1	100.9002.9107.142	4	0	9107	\N	100	142	恢复员工权限	142	\N
1	100.9002.9107.143	4	0	9107	\N	100	143	查询员工权限	143	\N
1	100.9002.9105.132	4	0	9105	\N	100	132	新增角色	132	\N
1	100.9002.9101.121	4	0	9101	\N	100	121	修改部门类型	121	\N
1	100.160	2	0	100	101	100	160	微信登录	160	\N
1	100.9002.9101.122	4	0	9101	\N	100	122	合并部门类型	122	\N
1	100.9002.9101.123	4	0	9101	\N	100	123	删除部门类型	123	\N
1	100.9002.9101.124	4	0	9101	\N	100	124	恢复部门类型	124	\N
1	100.9002.9101.125	4	0	9101	\N	100	125	查询部门类型	125	\N
1	100.9003.9111.183	4	0	9111	\N	100	183	修改省	183	\N
1	100.9003.9111.184	4	0	9111	\N	100	184	合并省	184	\N
1	100.9003.9111.185	4	0	9111	\N	100	185	删除省	185	\N
1	100.9001.9104.131	4	0	9104	\N	100	131	查询员工所属系统	131	\N
1	100.9002.9107.141	4	0	9107	\N	100	141	删除员工权限	141	\N
1	100.9001.9103.103	4	0	9103	\N	100	103	合并子系统	103	\N
1	100.9001.9103.104	4	0	9103	\N	100	104	删除子系统	104	\N
1	100.9002.9102.116	4	0	9102	\N	100	116	合并部门	116	\N
1	100.9001.9104.130	4	0	9104	\N	100	130	恢复员工所属系统	130	\N
1	100.9001.9100.109	4	0	9100	\N	100	109	修改系统权限	109	\N
1	100.9001.9100.110	4	0	9100	\N	100	110	合并系统权限	110	\N
1	100.9001.9109	3	1	9001	\N	100	9109	出错代码	\N	\N
1	100.9001.9100	3	6	9001	\N	100	9100	系统权限	\N	\N
1	100.9002.9106.149	4	0	9106	\N	100	149	查询员工部门	149	\N
1	100.9001.9103.101	4	0	9103	\N	100	101	新增子系统	101	\N
1	100.161	2	0	100	\N	100	161	注册	161	\N
1	100.9001.9103.102	4	0	9103	\N	100	102	修改子系统	102	\N
1	100.9001.9108	3	1	9001	\N	100	9108	权限名	\N	\N
1	100.9001.9104.128	4	0	9104	\N	100	128	合并员工所属系统	128	\N
1	100.9001.9104.129	4	0	9104	\N	100	129	删除员工所属系统	129	\N
1	100.9002.9107.139	4	0	9107	\N	100	139	修改员工权限	139	\N
1	100.9002.9107.140	4	0	9107	\N	100	140	合并员工权限	140	\N
1	100.9003.9111.186	4	0	9111	\N	100	186	恢复省	186	\N
1	100.9001.9110	3	1	9001	\N	100	9110	系统日志	\N	\N
1	100.9001.9108.180	4	0	9108	\N	100	180	查询权限名	180	\N
1	100.9001.9109.181	4	0	9109	\N	100	181	查询出错代码	181	\N
1	100.9002.9105.135	4	0	9105	\N	100	135	删除角色	135	\N
1	100.9002.9105.136	4	0	9105	\N	100	136	恢复角色	136	\N
\.


--
-- Data for Name: sysoper; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.sysoper (operatorid, systemid) FROM stdin;
100000	100
\.


--
-- Data for Name: sysoperorg; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.sysoperorg (operatorid, sysorgid) FROM stdin;
100000	100
\.


--
-- Data for Name: sysorg; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.sysorg (isused, idpath, idlevel, idcount, upid, systemid, sysorgid, sysorgname, description, orgtype) FROM stdin;
1	100	1	0	0	100	100	\N	\N	\N
\.


--
-- Data for Name: systeminfo; Type: TABLE DATA; Schema: sysinfo; Owner: gm
--

COPY sysinfo.systeminfo (systemid, systemname, isused, algorithm, prikey, createoperator, createtime, updateoperator, updatetime, deloperator, deltime, loginname) FROM stdin;
100	维护系统	1	1	Gao@12345	\N	\N	\N	\N	\N	\N	100
\.


--
-- Name: sysoperorg_sysorgid_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.sysoperorg_sysorgid_seq', 100, false);


--
-- Name: sysorg_sysorgid_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.sysorg_sysorgid_seq', 100, false);


--
-- Name: systeminfo_systemid_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.systeminfo_systemid_seq', 100, false);


--
-- Name: city_cid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('public.city_cid_seq', 100, false);



--
-- Name: operinfo_operatorid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.operinfo_operatorid_seq', 100020, true);


--
-- Name: operpermission_permissionid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.operpermission_permissionid_seq', 100, false);


--
-- Name: orgtype_orgtypeid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.orgtype_orgtypeid_seq', 110, true);


--
-- Name: province_pid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('public.province_pid_seq', 109, true);


--
-- Name: roleinfo_roleinfoid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.roleinfo_roleinfoid_seq', 109, true);


--
-- Name: rolepermission_permissionid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.rolepermission_permissionid_seq', 100, false);



--
-- Name: sysaction_sysactionid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.sysaction_sysactionid_seq', 100000009, true);


--
-- Name: sysoperorg_sysorgid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.sysoperorg_sysorgid_seq', 100, false);


--
-- Name: sysorg_sysorgid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.sysorg_sysorgid_seq', 110, true);


--
-- Name: systeminfo_systemid_seq; Type: SEQUENCE SET; Schema: sysinfo; Owner: gm
--

SELECT pg_catalog.setval('sysinfo.systeminfo_systemid_seq', 100, false);


--
-- Name: actions actions_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.actions
    ADD CONSTRAINT actions_pkey PRIMARY KEY (actionid);


--
-- Name: appparams appparams_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.appparams
    ADD CONSTRAINT appparams_pkey PRIMARY KEY (appid);


--
-- Name: city city_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY public.city
    ADD CONSTRAINT city_pkey PRIMARY KEY (cid);


--
-- Name: operpermission employeepermission_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.operpermission
    ADD CONSTRAINT employeepermission_pkey PRIMARY KEY (permissionid);


--
-- Name: errorcode errorcode_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.errorcode
    ADD CONSTRAINT errorcode_pkey PRIMARY KEY (errorcode);


--
-- Name: funchtml funchtml_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.funchtml
    ADD CONSTRAINT funchtml_pkey PRIMARY KEY (htmlid);


-- (sysinfo.loginlog 主键已随表并入 public.loginlog，2026-11-13 日志归拢)


--
-- Name: operinfo operinfo_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.operinfo
    ADD CONSTRAINT operinfo_pkey PRIMARY KEY (operatorid);


--
-- Name: orgtype orgtype_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.orgtype
    ADD CONSTRAINT orgtype_pkey PRIMARY KEY (orgtypeid);


--
-- Name: province province_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY public.province
    ADD CONSTRAINT province_pkey PRIMARY KEY (pid);


--
-- Name: roleinfo roleinfo_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.roleinfo
    ADD CONSTRAINT roleinfo_pkey PRIMARY KEY (roleinfoid);


--
-- Name: rolepermission rolepermission_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.rolepermission
    ADD CONSTRAINT rolepermission_pkey PRIMARY KEY (permissionid);


-- (sysinfo.serverlog 主键已随表并入 public.serverlog，2026-11-13 日志归拢)

--
-- Name: sysaction sysaction_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.sysaction
    ADD CONSTRAINT sysaction_pkey PRIMARY KEY (sysactionid);


--
-- Name: sysoper sysoper_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.sysoper
    ADD CONSTRAINT sysoper_pkey PRIMARY KEY (operatorid, systemid);


--
-- Name: sysoperorg sysoperorg_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.sysoperorg
    ADD CONSTRAINT sysoperorg_pkey PRIMARY KEY (operatorid, sysorgid);


--
-- Name: sysorg sysorg_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.sysorg
    ADD CONSTRAINT sysorg_pkey PRIMARY KEY (sysorgid);


--
-- Name: systeminfo systeminfo_pkey; Type: CONSTRAINT; Schema: sysinfo; Owner: gm
--

ALTER TABLE ONLY sysinfo.systeminfo
    ADD CONSTRAINT systeminfo_pkey PRIMARY KEY (systemid);


--
-- Name: SCHEMA gm; Type: ACL; Schema: -; Owner: gm
--

GRANT USAGE ON SCHEMA gm TO conn;


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO postgres;


--
-- Name: SCHEMA sysinfo; Type: ACL; Schema: -; Owner: gm
--

GRANT USAGE ON SCHEMA sysinfo TO conn;


--
-- Name: TABLE nginx; Type: ACL; Schema: gm; Owner: gm
--

REVOKE ALL ON TABLE gm.nginx FROM gm;
GRANT SELECT,INSERT,REFERENCES,DELETE,TRIGGER,TRUNCATE,UPDATE ON TABLE gm.nginx TO gm;
GRANT SELECT ON TABLE gm.nginx TO conn;



--
-- PostgreSQL database dump complete
--

\unrestrict s5h1yJp7hfkcujfahXrb8hwC5ODsXL5gcxziIyLenTnnUgDjbaby6cHbravD9AK


--
-- Name: operator; Type: SCHEMA; Schema: -; Owner: gm
--

CREATE SCHEMA operator;
ALTER SCHEMA operator OWNER TO gm;

--
-- Name: changepassword(p_token, p_oldpass, p_newpass); Type: FUNCTION; Schema: operator; Owner: gm
--
-- 说明：为对齐 bootstrap 版前端的“修改密码”功能(operator/changepassword)补充。
-- 逻辑：校验登录态(权限点141) → 校验旧密码 → 更新 sysinfo.operinfo.pass。
--

CREATE OR REPLACE FUNCTION operator.changepassword(p_token character varying, p_oldpass character varying, p_newpass character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_check json;v_operatorid int;v_c int;
begin
  v_check:=gm.check_login(p_token,141);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  v_operatorid:=(v_check->'info'->>'operatorid')::integer;
  if p_oldpass is null or p_newpass is null then
    return gm.returnjson(100010); --密码不能为空
  end if;
  if length(p_newpass)<8 or p_newpass !~ '[A-Za-z]' or p_newpass !~ '[0-9]'
     or p_newpass like '%123456%' or p_newpass in ('12345678','password','qwerty123','11111111','88888888','admin123456') then
    return gm.returnjson(100035); --口令复杂度不足
  end if;
  select count(*) into v_c from sysinfo.operinfo where operatorid=v_operatorid and pass=crypto.crypt(p_oldpass,pass) and isused=1;
  if v_c<>1 then
    return gm.returnjson(100001); --旧密码不正确
  end if;
  update sysinfo.operinfo set pass=p_newpass where operatorid=v_operatorid;--明文由哈希触发器自动转换
  return gm.returnjson(0);
end
$$;

ALTER FUNCTION operator.changepassword(p_token character varying, p_oldpass character varying, p_newpass character varying) OWNER TO gm;

--
-- 接口文档（2026-11-13，同日更新）：
--   1) 权限节点 9113“接口文档”，挂在“系统(100)”之下（idpath=100.9113）；
--      admin 经根节点 100 的授权自动获得该权限（check_login 按 idpath 前缀匹配）。
--   2) sysinfo.help_query（原 gethelp 改名）：返回当前库所有业务函数的元数据(JSON)，
--      仅业务 schema；排除 crypto/gm（安全与内部工具函数）及 information_schema/pg_catalog，
--      替代原 gm.gethelp()（返回整页 HTML、依赖 sysinfo.funchtml 模板且无鉴权，已删除）。
--   3) sysinfo.help_edit（权限 224，挂于 9113 之下）：修改指定函数的注释
--      （pg_description），前端接口文档页可在线编辑说明。
--   4) 前端在“全系统管理”分组新增“接口文档”页面渲染该数据。
--

INSERT INTO sysinfo.sysaction (isused, idpath, idlevel, idcount, upid, params, systemid, sysactionid, sysactionname, actionid, isdefault)
VALUES (1, '100.9113', 2, 0, 100, NULL, 100, 9113, '接口文档', NULL, NULL)
ON CONFLICT (sysactionid) DO NOTHING;

INSERT INTO sysinfo.sysaction (isused, idpath, idlevel, idcount, upid, params, systemid, sysactionid, sysactionname, actionid, isdefault)
VALUES (1, '100.9113.224', 4, 0, 9113, NULL, 100, 224, '修改接口注释', 224, NULL)
ON CONFLICT (sysactionid) DO NOTHING;

CREATE OR REPLACE FUNCTION sysinfo.help_query(p_token character varying) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_check json;v_rows json;
begin
  v_check:=gm.check_login(p_token,9113);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  select array_to_json(array_agg(row_to_json(t))) into v_rows
    from (select n.nspname schemaname, p.proname funcname,
                 pg_get_function_arguments(p.oid) arguments,
                 pg_get_function_result(p.oid) returntype,
                 coalesce(obj_description(p.oid,'pg_proc'),'') description
            from pg_proc p join pg_namespace n on p.pronamespace=n.oid
           where n.nspname<>'information_schema' and n.nspname not like 'pg\_%'
             and n.nspname not in ('crypto','gm')
             and p.prokind='f'
           order by n.nspname,p.proname) t;
  return gm.returnjson(0,json_build_object('total',coalesce(json_array_length(v_rows),0),'rows',v_rows));
end
$$;

ALTER FUNCTION sysinfo.help_query(p_token character varying) OWNER TO gm;

CREATE OR REPLACE FUNCTION sysinfo.help_edit(p_token character varying, p_schemaname character varying, p_funcname character varying, p_description text, p_arguments text) RETURNS json
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_check json;v_oid bigint;
begin
  v_check:=gm.check_login(p_token,224);
  if v_check->>'errorcode'<>'0' then return v_check; end if;
  if p_schemaname is null or p_funcname is null then return gm.returnjson(100010); end if;--参数不能为空
  if p_arguments is not null then
    select p.oid into v_oid from pg_proc p join pg_namespace n on p.pronamespace=n.oid
     where n.nspname=p_schemaname and p.proname=p_funcname
       and pg_get_function_arguments(p.oid)=p_arguments limit 1;
  end if;
  if v_oid is null then
    select min(p.oid) into v_oid from pg_proc p join pg_namespace n on p.pronamespace=n.oid
     where n.nspname=p_schemaname and p.proname=p_funcname;
  end if;
  if v_oid is null then return gm.returnjson(100018); end if;--函数不存在
  update pg_description set description=coalesce(p_description,'')
   where classoid='pg_proc'::regclass and objoid=v_oid and objsubid=0;
  if not found then
    insert into pg_description(classoid,objoid,objsubid,description)
    values ('pg_proc'::regclass, v_oid, 0, coalesce(p_description,''));
  end if;
  return gm.returnjson(0,json_build_object('id',v_oid));
end
$$;

ALTER FUNCTION sysinfo.help_edit(p_token character varying, p_schemaname character varying, p_funcname character varying, p_description text, p_arguments text) OWNER TO postgres;

--
-- 等级保护 2.0 合规整改（2026-11-13）：
--   1) 口令存储：operinfo.pass 一律 blowfish 哈希（crypto.crypt），存量明文由触发器
--      与迁移脚本自动转换；登录/改密用 crypt(输入, 存量哈希) 比对，不再明文比对。
--   2) 登录日志：loginlog 不再记录口令（pass 列已删除）。
--   3) 操作审计：核心权限/员工/系统表写入时由触发器自动记入 serverlog
--      （时间、客户端IP、对象、操作类型、数据快照、操作者）。
--   4) 新增错误码 100035：口令复杂度不足（≥8 位且含字母和数字，拒绝常见弱口令）。
--   5) 最小权限：nginx postgres 模块改用既有账号 conn 连接（serverlog INSERT 已授），
--      仅授予函数 EXECUTE 权限，不授予业务表直接读写权限；原专用账号 ngxapp 已删除。
--

INSERT INTO sysinfo.errorcode (message, errorcode, primekey, isused, schema)
SELECT '系统内置操作员不能删除！', 100036, '100036', 1, 'sysinfo'
WHERE NOT EXISTS (SELECT 1 FROM sysinfo.errorcode WHERE errorcode=100036);

INSERT INTO sysinfo.errorcode (message, errorcode, primekey, isused, schema)
SELECT '口令复杂度不足：至少8位且须同时包含字母和数字，禁止常见弱口令', 100035, '100035', 1, 'sysinfo'
WHERE NOT EXISTS (SELECT 1 FROM sysinfo.errorcode WHERE errorcode=100035);

GRANT USAGE ON SCHEMA crypto TO gm;

CREATE OR REPLACE FUNCTION gm.hash_pass_trigger() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
begin
  if NEW.pass is not null and NEW.pass<>'' and NEW.pass not like '$2%' then
    NEW.pass:=crypto.crypt(NEW.pass, crypto.gen_salt('bf',8));
  end if;
  return NEW;
end
$$;

ALTER FUNCTION gm.hash_pass_trigger() OWNER TO postgres;

DROP TRIGGER IF EXISTS trg_hash_pass ON sysinfo.operinfo;
CREATE TRIGGER trg_hash_pass BEFORE INSERT OR UPDATE OF pass ON sysinfo.operinfo
    FOR EACH ROW EXECUTE FUNCTION gm.hash_pass_trigger();

CREATE OR REPLACE FUNCTION gm.audit_trigger() RETURNS trigger
    LANGUAGE plpgsql SECURITY DEFINER
    AS $$
declare v_oper int;v_row text;
begin
  v_oper:=nullif(current_setting('app.operatorid', true),'')::int;
  v_row:=case when TG_OP='DELETE' then row_to_json(OLD)::text else row_to_json(NEW)::text end;
  begin
    insert into public.serverlog(clientip,pckname,funcname,content,logtime,serverip,params,operatorid,res,head)
    values (inet_client_addr(),'audit',TG_TABLE_NAME||'.'||TG_OP,left(v_row,2000),now(),inet_server_addr(),null,v_oper,'ok',null);
  exception when others then
    null;--审计写入失败不阻断业务
  end;
  return coalesce(NEW,OLD);
end
$$;

ALTER FUNCTION gm.audit_trigger() OWNER TO postgres;

COMMENT ON FUNCTION gm.audit_trigger() IS '等保审计触发器（AFTER INSERT/UPDATE/DELETE）：把核心权限/员工/系统表变更写入 public.serverlog（pckname=audit）
操作者取自 app.operatorid（由 gm.check_login 设置）；content 为行快照（截断2000字符）；审计写入失败不阻断业务（异常吞掉）
返回：coalesce(NEW,OLD)';

-- trg_audit on public.loginlog 在文末 public.loginlog 建表后创建（日志归拢）
DROP TRIGGER IF EXISTS trg_audit ON sysinfo.operinfo;
CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE OR DELETE ON sysinfo.operinfo
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();
DROP TRIGGER IF EXISTS trg_audit ON sysinfo.sysaction;
CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE OR DELETE ON sysinfo.sysaction
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();
DROP TRIGGER IF EXISTS trg_audit ON sysinfo.roleinfo;
CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE OR DELETE ON sysinfo.roleinfo
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();
DROP TRIGGER IF EXISTS trg_audit ON sysinfo.systeminfo;
CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE OR DELETE ON sysinfo.systeminfo
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();
DROP TRIGGER IF EXISTS trg_audit ON sysinfo.operpermission;
CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE OR DELETE ON sysinfo.operpermission
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();
DROP TRIGGER IF EXISTS trg_audit ON sysinfo.rolepermission;
CREATE TRIGGER trg_audit AFTER INSERT OR UPDATE OR DELETE ON sysinfo.rolepermission
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();

-- public.serverlog：统一日志表（nginx 模块请求日志 + gm.audit_trigger 操作审计 pckname='audit'）。
-- 等保审计核心表；按月分区就绪（PARTITION BY RANGE (logtime)），海量数据下按月建子表+归档。
CREATE TABLE public.serverlog (
    logid bigint NOT NULL,
    clientip inet,
    pckname character varying(255),
    funcname character varying(255),
    content text,
    logtime timestamp without time zone,
    serverip inet,
    params text,
    operatorid bigint,
    res text,
    head text
);
ALTER TABLE public.serverlog OWNER TO postgres;
CREATE SEQUENCE public.serverlog_logid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.serverlog_logid_seq OWNER TO postgres;
ALTER SEQUENCE public.serverlog_logid_seq OWNED BY public.serverlog.logid;

-- public.loginlog：登录日志（登录成功/失败均记录；不记录口令——等保要求）。
-- 由 sysinfo.login/loginaccount 写入；logid 由本序列生成（原 sysinfo 序列 +100000 平移）。
CREATE TABLE public.loginlog (
    logid bigint NOT NULL,
    logintime timestamp without time zone,
    operatorno bigint,
    ip inet,
    accounts character varying(255),
    systemid smallint
);
ALTER TABLE public.loginlog OWNER TO postgres;
CREATE SEQUENCE public.loginlog_logid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.loginlog_logid_seq OWNER TO postgres;
ALTER SEQUENCE public.loginlog_logid_seq OWNED BY public.loginlog.logid;
ALTER TABLE ONLY public.loginlog
    ADD CONSTRAINT loginlog_pkey PRIMARY KEY (logid);
COMMENT ON TABLE public.loginlog IS '登录日志（成功/失败均记录，不含口令）';

DROP TRIGGER IF EXISTS trg_audit ON public.loginlog;
CREATE TRIGGER trg_audit AFTER INSERT OR DELETE ON public.loginlog
    FOR EACH ROW EXECUTE FUNCTION gm.audit_trigger();

-- 连接账号统一使用既有角色 conn（nginx postgres 模块连接账号）。
GRANT INSERT ON TABLE public.serverlog TO conn;                       -- 模块请求日志表
GRANT USAGE, SELECT ON SEQUENCE public.serverlog_logid_seq TO conn;   -- 请求日志 logid 序列
GRANT INSERT ON TABLE public.loginlog TO conn;                        -- 登录日志表（login/loginaccount 写入）
GRANT USAGE, SELECT ON SEQUENCE public.loginlog_logid_seq TO conn;    -- 登录日志 logid 序列
-- gm（SECURITY DEFINER 函数属主）读写日志表：login() 查登录锁定计数并写 loginlog
GRANT SELECT, INSERT ON TABLE public.loginlog TO gm;
GRANT USAGE, SELECT ON SEQUENCE public.loginlog_logid_seq TO gm;
GRANT SELECT ON TABLE public.serverlog TO gm;                         -- 备用（审计/排查）

-- 2026-11-13 日志归拢：若存在旧表 sysinfo.serverlog(操作审计)/sysinfo.loginlog(登录日志)，
-- 则迁入 public.serverlog / public.loginlog（logid 统一 +100000 偏移避让；可幂等重放）。
-- 全新安装（无旧表）时跳过，序列从 1 起。
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='sysinfo' AND tablename='serverlog') THEN
    INSERT INTO public.serverlog(logid,clientip,pckname,funcname,content,logtime,serverip,params,operatorid,res,head)
    SELECT s.logid+100000,s.clientip,s.pckname,s.funcname,s.content,s.logtime,s.serverip,s.params,s.operatorid,s.res,s.head
    FROM sysinfo.serverlog s
    WHERE NOT EXISTS (SELECT 1 FROM public.serverlog p WHERE p.logid=s.logid+100000);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='sysinfo' AND tablename='loginlog') THEN
    INSERT INTO public.loginlog(logid,logintime,operatorno,ip,accounts,systemid)
    SELECT s.logid+100000,s.logintime,s.operatorno,s.ip,s.accounts,s.systemid
    FROM sysinfo.loginlog s
    WHERE NOT EXISTS (SELECT 1 FROM public.loginlog p WHERE p.logid=s.logid+100000);
  END IF;
  PERFORM setval('public.serverlog_logid_seq', GREATEST((SELECT COALESCE(max(logid),1) FROM public.serverlog),1), true);
  PERFORM setval('public.loginlog_logid_seq', GREATEST((SELECT COALESCE(max(logid),1) FROM public.loginlog),1), true);
END $$;
-- 2026-11-13 二次清理：确认 public 侧数据完整后 DROP 旧表与旧序列（幂等；本 dump 已不含其 CREATE，重放即一致）。
DROP TABLE IF EXISTS sysinfo.serverlog;
DROP SEQUENCE IF EXISTS sysinfo.serverlog_logid_seq;
DROP TABLE IF EXISTS sysinfo.loginlog;
DROP SEQUENCE IF EXISTS sysinfo.loginlog_logid_seq;
GRANT USAGE ON SCHEMA sysinfo TO conn;
GRANT USAGE ON SCHEMA gm TO conn;
GRANT USAGE ON SCHEMA operator TO conn;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sysinfo TO conn;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA gm TO conn;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA conn TO conn;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA operator TO conn;
ALTER DEFAULT PRIVILEGES IN SCHEMA sysinfo GRANT EXECUTE ON FUNCTIONS TO conn;
ALTER DEFAULT PRIVILEGES IN SCHEMA gm GRANT EXECUTE ON FUNCTIONS TO conn;
ALTER DEFAULT PRIVILEGES IN SCHEMA operator GRANT EXECUTE ON FUNCTIONS TO conn;

-- 移除原专用连接账号 ngxapp（先回收其名下授权再删除；幂等：不存在则跳过）
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='ngxapp') THEN
    DROP OWNED BY ngxapp;
    DROP ROLE ngxapp;
  END IF;
END
$$;

-- 重放收尾：初始数据使用固定 permissionid（如 100/102），序列须同步到既有数据最大值，
-- 否则全新安装后 nextval 生成的 id 会与初始数据冲突（-operpermission 主键重复）。
SELECT setval('sysinfo.operpermission_permissionid_seq',
              COALESCE((SELECT max(permissionid) FROM sysinfo.operpermission), 1), true);
-- ==================== 外键引用元数据表 public.fk_meta（建模生成模板用） ====================
CREATE TABLE IF NOT EXISTS public.fk_meta (
  id bigserial primary key,
  master_schema character varying(63) NOT NULL,
  master_table character varying(63) NOT NULL,
  master_column character varying(63) NOT NULL,
  master_multi smallint DEFAULT 0,
  fk_schema character varying(63) NOT NULL,
  fk_table character varying(63) NOT NULL,
  fk_column character varying(63) NOT NULL,
  display_columns character varying(255) DEFAULT '',
  master_module character varying(63) DEFAULT '',
  isused smallint DEFAULT 1,
  createtime timestamp DEFAULT now()
);
ALTER TABLE public.fk_meta OWNER TO postgres;
COMMENT ON TABLE public.fk_meta IS '外键引用元数据（建模生成模板用）：主模式/主表/主字段(被引用方)、主字段是否可多选(1=可多选,逗号分隔)、外键模式/外键表/外键字段(引用方)、主表显示字段(逗号分隔一个或多个)、主表前端模块id';
CREATE UNIQUE INDEX IF NOT EXISTS fk_meta_uniq ON public.fk_meta(fk_schema,fk_table,fk_column);
GRANT SELECT ON TABLE public.fk_meta TO conn;
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.fk_meta TO gm;
GRANT USAGE,SELECT ON SEQUENCE public.fk_meta_id_seq TO conn,gm;

INSERT INTO sysinfo.errorcode(errorcode,message,primekey,schema,isused)
SELECT 100039,'关联数据不存在！','100039','sysinfo',1 WHERE NOT EXISTS (SELECT 1 FROM sysinfo.errorcode WHERE errorcode=100039);
-- ==================== fk_meta 增加主表所在数据库字段（分库/分表场景） ====================
ALTER TABLE public.fk_meta ADD COLUMN IF NOT EXISTS master_db character varying(63) DEFAULT 'local';
COMMENT ON COLUMN public.fk_meta.master_db IS '主表所在数据库：local=本机(默认)；其它=外部库，经 postgres_fdw 映射到本地 fdwext schema 后访问';
