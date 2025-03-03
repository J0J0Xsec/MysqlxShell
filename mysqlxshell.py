import mysqlx

# MySQLX 连接配置
connection_config = {
    "host": "",
    "port": 33060,
    "user": "",
    "password": "",
    "connect_timeout": 150  # 设置连接超时
}

try:
    # 连接到 MySQLX
    print("连接到 MySQLX...")
    session = mysqlx.get_session(connection_config)
    print("✅ 已连接到 MySQLX 服务器！")

    # 显示所有数据库
    result = session.sql("SHOW DATABASES").execute()
    databases = result.fetch_all()

    if not databases:
        print("❌ 未找到数据库，程序退出！")
        session.close()
        exit()

    # 解析数据库列表
    db_list = [db[0] for db in databases]
    
    # 让用户选择数据库
    print("\n📌 请选择一个数据库：")
    for i, db in enumerate(db_list, 1):
        print(f"  {i}. {db}")
    
    selected_db = None
    while selected_db is None:
        try:
            choice = input("请输入编号选择数据库: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(db_list):
                selected_db = db_list[int(choice) - 1]
                print(f"✅ 已选择数据库: {selected_db}")
            else:
                print("❌ 选择无效，请重新输入！")
        except Exception as e:
            print(f"❌ 选择数据库时出错: {e}")
            break

    # 切换数据库
    session.sql(f"USE {selected_db}").execute()
    print(f"✅ 已切换到数据库: {selected_db}\n")

    # 进入 SQL 交互模式
    while True:
        try:
            sql_query = input(f"MySQLX [{selected_db}]> ").strip()

            # 如果输入为空则跳过
            if not sql_query:
                print("❌ 输入不能为空，请输入有效 SQL。")
                continue

            if sql_query.lower() == "exit":
                print("🔌 断开连接，退出程序。")
                break

            # 处理 UPDATE/DELETE/SET 语句的卡顿问题
            print("⏳ SQL 执行中...")
            result = session.sql(sql_query).execute()

            # 检查是否是数据变更语句
            if sql_query.lower().startswith(("update", "delete", "set", "insert")):
                affected_rows = result.get_affected_items_count()
                print(f"✅ SQL 执行成功，受影响行数: {affected_rows}")

                # 如果 MySQL 运行在事务模式，手动提交
                session.sql("COMMIT;").execute()
                print("✅ 事务已提交！")

            else:
                # 获取查询结果
                rows = result.fetch_all()
                columns = result.get_columns()

                if columns:
                    # 获取字段名
                    col_names = [col.get_column_name() for col in columns]
                    
                    # 计算列宽
                    col_widths = [max(len(str(val)) for val in [name] + [row[i] for row in rows]) for i, name in enumerate(col_names)]

                    # 打印表头
                    print(" | ".join(name.ljust(width) for name, width in zip(col_names, col_widths)))
                    print("-" * sum(col_widths) + "-" * (3 * (len(col_widths) - 1)))

                    # 打印数据
                    for row in rows:
                        print(" | ".join(str(val).ljust(width) for val, width in zip(row, col_widths)))
                else:
                    print("✅ SQL 执行成功，无返回数据。")

        except Exception as e:
            print(f"❌ SQL 执行错误: {e}")

except Exception as e:
    print(f"🚨 连接失败: {e}")

finally:
    if 'session' in locals() and session.is_open():
        session.close()
        print("🔌 连接已关闭。")
