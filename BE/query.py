import sqlite3
import json, os

# db_path = "/home/lam/Documents/prj3_copy/prj3/data/db.sqlite3"
basedir = os.path.dirname(os.path.dirname((os.path.dirname(__file__))))
db_path = os.path.join(basedir, 'Data_Project3')

def create_sub_tables(num_records, topic):
    with sqlite3.connect(db_path + '/db.sqlite3') as conn:
        cur = conn.cursor()
        cur.execute("ATTACH DATABASE '" + db_path +  "/subDB_" + num_records + "_" + topic + ".sqlite3' AS sub_db")
        name1 = "paper"
        name2 = "paper_authors"
        name3 = "author" 
        check_paper = "select count(name) from sub_db.sqlite_master where type='table' and name='paper'"
        check_paper_author = "select count(name) from sub_db.sqlite_master where type='table' and name='paper_authors'"
        check_author = "select count(name) from sub_db.sqlite_master where type='table' and name='author'"
        
        create_paper_table = ("create table sub_db.paper" +  
                            " as select p.id, p.date from collab_paper p \
                            where p.journal_id = " + topic +
                            " limit " + num_records
                            )

        create_paper_authors_table = ("create table sub_db.paper_authors" +  
                            " as select pa.* from collab_paper_authors pa \
                            where pa.paper_id in (select id from sub_db.paper)"
                            )

        create_author_table = ("create table sub_db.author" +  
                            " as select a.id, a.affiliation_id, ins.university, a.country_id from collab_author a \
                              left join collab_institute ins \
                              on a.affiliation_id = ins.id  \
                            where a.id in (select distinct pa.author_id from sub_db.paper_authors pa)"
                            )       

        cur.execute(check_paper)
        if cur.fetchone()[0] == 1:
            msg = "sub database paper already exists"
        else:
            cur.execute(create_paper_table)
            cur.execute(check_paper_author)
            if cur.fetchone()[0] == 1:
                msg = "sub database paper_author already exists"
            else:
                cur.execute(create_paper_authors_table)
                cur.execute(check_author)
                if cur.fetchone()[0] == 1:
                    msg = "sub database author already exists" 
                else:
                    cur.execute(create_author_table)
                    msg = "create sub database successfully!"

        # query to return result ==> may be unnecessary
        query1 = ("select * from sub_db.paper")
        query2 = ("select * from sub_db.author")
        query3 = ("select * from sub_db.paper_authors")
        cur.execute(query1)
        name_1 = [i[0] for i in cur.description]
        res1 = cur.fetchall()
        cur.execute(query2)
        name_2 = [i[0] for i in cur.description]
        res2 = cur.fetchall()
        cur.execute(query3)
        name_3 = [i[0] for i in cur.description]
        res3 = cur.fetchall()
    return  json.dumps({"paper": [name_1,res1], "author": [name_2,res2], "paper_authors": [name_3,res3]})

def create_co_authors(num_records, topic):
    with sqlite3.connect(db_path + '/db.sqlite3') as conn:
        cur = conn.cursor()
        cur.execute("ATTACH DATABASE '" + db_path +  "/subDB_" + num_records + "_" + topic + ".sqlite3' AS sub_db")
        create_co_authors_table = ("create table sub_db.co_author \
                                    as select pa1.paper_id, pa1.author_id as id_author_1, pa2.author_id as id_author_2 \
                                    from sub_db.paper_authors pa1 \
                                    join sub_db.paper_authors pa2 \
                                    on pa1.paper_id = pa2.paper_id and pa1.author_id < pa2.author_id order by pa1.paper_id"
                                )       
        check = "select count(name) from sub_db.sqlite_master where type='table' and name='co_author'"
        cur.execute(check)
        if cur.fetchone()[0] == 1:
            msg = "co author table already exists"
            message ={ "msg" : msg, "name": "co_author"}
        else:
            res = cur.execute(create_co_authors_table)
            message = {"msg": "create co author table successfully!",
                    "name": "co author " + num_records + "_" + topic
            }

    # query to return result ==> may be unnecessary
    query = ("select * from sub_db.co_author")
    cur.execute(query)
    column_names = [i[0] for i in cur.description]
    result = cur.fetchall()
    return json.dumps({"co_author": [column_names,result], "msg": message})

def create_potential_co_authors(num_records, level, topic):
    temp = "sub_db.co_author"
    message = []

    with sqlite3.connect(db_path + '/db.sqlite3') as conn:
        cur = conn.cursor()
        cur.execute("ATTACH DATABASE '" + db_path +  "/subDB_" + num_records + "_" + topic + ".sqlite3' AS sub_db")
        for i in range(level):
            if i > 0:
                temp = "sub_db.potential_co_author_" + str(i)
            create_potential_co_authors_table = ("create table sub_db.potential_co_author_" + str(i+1) 
                                            + " as \
                                                select co1.id_author_2 as id_author_1, co2.id_author_2 as id_author_2 \
                                                from " + temp + " co1 \
                                                join " + temp + " co2 \
                                                on co1.id_author_1 = co2.id_author_1 \
                                                and co1.id_author_2 < co2.id_author_2 \
                                                union \
                                                select co2.id_author_1 as id_author_1, co1.id_author_2 as id_author_2 \
                                                from " + temp + " co1 \
                                                join " + temp + " co2 \
                                                on co1.id_author_1 = co2.id_author_2 \
                                                and co1.id_author_2 > co2.id_author_1 \
                                                union \
                                                select co1.id_author_1 as id_author_1, co2.id_author_1 as id_author_2 \
                                                from " + temp + " co1 \
                                                join " + temp + " co2 \
                                                on co1.id_author_2 = co2.id_author_2 \
                                                and co1.id_author_1 < co2.id_author_1 \
                                                union \
                                                select co.id_author_1, co.id_author_2 \
                                                from " + temp + " co")
            name = "potential_co_author_" + str(i+1) 
            check = "select count(name) from sub_db.sqlite_master where type='table' and name='" + name + "'"
            cur.execute(check)
            if cur.fetchone()[0] == 1:
                msg = name + " already exists"
                message.append({"msg": msg})
            else:
                cur.execute(create_potential_co_authors_table)
                msg = "create potential co author table level " + str(i+1) + " successfully"   
                message.append({"msg" : msg})

    # query to return result ==> may be unnecessary
    query = ("select * from sub_db.potential_co_author_" + str(level))
    cur.execute(query)
    column_names = [i[0] for i in cur.description]
    result = cur.fetchall()
    return json.dumps({"last_potential": [column_names,result], "msg": message})