-- 序列同步 v3：
--  1) sysinfo 业务序列：兼容 MINVALUE=100（max 可能小于 start），取 GREATEST(max, min_value)；
--  2) public 日志序列（serverlog/loginlog，2026-11-13 日志归拢后新序列从 1 起）：
--     dump 重放后日志行已含 +100000 平移的审计/登录数据，序列拨到 max。
DO $$
DECLARE
    pairs text[][] := ARRAY[
        ['operpermission_permissionid_seq','operpermission','permissionid'],
        ['rolepermission_permissionid_seq','rolepermission','permissionid'],
        ['operinfo_operatorid_seq','operinfo','operatorid'],
        ['roleinfo_roleinfoid_seq','roleinfo','roleinfoid'],
        ['orgtype_orgtypeid_seq','orgtype','orgtypeid'],
        ['province_pid_seq','province','pid'],
        ['city_cid_seq','city','cid'],
        ['sysaction_sysactionid_seq','sysaction','sysactionid'],
        ['sysorg_sysorgid_seq','sysorg','sysorgid'],
        ['systeminfo_systemid_seq','systeminfo','systemid']
    ];
    pubpairs text[][] := ARRAY[
        ['serverlog_logid_seq','serverlog','logid'],
        ['loginlog_logid_seq','loginlog','logid']
    ];
    i int; mx bigint; mn bigint;
BEGIN
    FOR i IN 1..array_length(pairs,1) LOOP
        BEGIN
            EXECUTE format('SELECT max(%I) FROM sysinfo.%I', pairs[i][3], pairs[i][2]) INTO mx;
            SELECT min_value INTO mn FROM pg_sequences WHERE schemaname='sysinfo' AND sequencename=pairs[i][1];
            IF mx IS NOT NULL THEN
                PERFORM setval('sysinfo.' || pairs[i][1], GREATEST(mx, COALESCE(mn,1)), true);
            END IF;
        EXCEPTION WHEN undefined_table OR undefined_column OR undefined_object THEN
            NULL;
        END;
    END LOOP;
    FOR i IN 1..array_length(pubpairs,1) LOOP
        BEGIN
            EXECUTE format('SELECT max(%I) FROM public.%I', pubpairs[i][3], pubpairs[i][2]) INTO mx;
            IF mx IS NOT NULL AND mx > 0 THEN
                PERFORM setval('public.' || pubpairs[i][1], mx, true);
            END IF;
        EXCEPTION WHEN undefined_table OR undefined_column OR undefined_object THEN
            NULL;
        END;
    END LOOP;
    RAISE NOTICE 'sequences synced v3';
END $$;
