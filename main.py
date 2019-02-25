import csv
import time
import random
import pyodbc
import redis
import hashlib
import pandas as pd
import pickle as cPickle
from flask import Flask, request, render_template

database = 'earthquake'
server = 'earthquake1.database.windows.net'
username = 'superhit69'
password = 'Bluebelt10'
driver = '{ODBC Driver 13 for SQL Server}'
app = Flask(__name__)
cachename = 'testQueryRes'

cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database
                      + ';UID=' + username + ';PWD=' + password)

rd = redis.StrictRedis(host='quake.redis.cache.windows.net', port=6380, db=0,
                       password='EBaPr0eSD5OzgkZxWsc3qjW1tBuI9l7Eo3NNiD4UPdQ=', ssl=True)


# cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
#                             + ';PORT=1443;DATABASE=' + database
#                             + ';UID=' + username + ';PWD=' + password)
# from flask import Flask

@app.route('/')
def my_form():
    return render_template('my-form.html')


@app.route('/', methods=['POST'])
def my_form_post():
    text = request.form['text']
    processed_text = text.upper()
    return processed_text

# -------- Normal Search from db --------
@app.route('/search', methods=['GET'])
def search():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                          + ';PORT=1443;DATABASE=' + database
                          + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()
    start_time = time.time()
    cursor.execute('SELECT * from quakes')
    rows = cursor.fetchall()
    print(rows)
    end_time = time.time()
    duration = end_time - start_time
    return render_template('index.html', ci=rows, timedur=duration)

# --------- Normal Range search ----
@app.route('/search1', methods=['POST'])
def range1():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database
                          + ';UID=' + username + ';PWD=' + password)

    print(request.form.get('mag_1'))
    cursor = cnxn.cursor()
    starttime = time.time()
    cursor.execute(
        'select mag from quakes where mag between ' + request.form.get('mag_1') + ' and ' + request.form.get('mag_2'))
    rows = cursor.fetchall()
    print(rows)
    endtime = time.time()
    duration = endtime - starttime
    return render_template('index.html', ci=rows, timedur=duration)

# -------- Normal Random number of queries ------
@app.route('/cachequeries', methods=['POST'])
def randomqueries():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database
                          + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()
    starttime = time.time()
    mag1 = request.form['mag1']
    mag2 = request.form['mag2']
    Queries = request.form['Queries']

    for i in range(0, int(Queries)):
        random1 = round(random.uniform(float(mag1), float(mag2)), 3)

        # print(random1)
        cursor.execute("select * from quakes where mag >'" + str(random1) + "'")
    rows = cursor.fetchall()
    print(rows)
    endtime = time.time()
    duration = endtime - starttime
    return render_template('index.html', ci=rows, timedur=duration)

# --------- INSERT TABLE INTO DB ----------
@app.route('/c', methods=['GET'])
def csvload():
    with open('quakes.csv', mode='r') as csv_file:
        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                              + ';PORT=1443;DATABASE=' + database
                              + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        starttime = time.time()
        for row in csv_reader:
            sql = "Insert into [dbo].[quakes] \
                        ([time], [latitude], [longitude], [depth], [mag], [magType], [nst], [gap], [dmin], [rms], [net],\
                        [id], [updated], [place], [type], [horizontalError], [depthError], [magError], [magNst], [status],\
                        [locationSource], [magSource]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            values = [row['time'], row['latitude'], row['longitude'], row['depth'],
                      row['mag'], row['magType'], row['nst'], row['gap'], row['dmin'], row['rms'], row['net'],
                      row['id'], row['updated'], row['place'], row['type'], row['horizontalError'],
                      row['depthError'],
                      row['magError'], row['magNst'], row['status'], row['locationSource'], row['magSource']]
            cursor.execute(sql, values)
            cursor.commit()
            line_count = line_count + 1
            print("updated record number {}".format(line_count))
            endtime = time.time()
            duration = endtime - starttime
            print(duration)

        return render_template("index.html", timedur=duration)


# --------- NORMAL RANGE SEARCH WITH REDIS ----------
@app.route('/quakeRangeRedis', methods=['GET'])
def quakeRangeRedis():
    if rd.exists(cachename):
        print('Cache found')
        start_time = time.time()
        results = cPickle.loads(rd.get(cachename))
        end_time = time.time()

    else:
        print('Cache Not Found')
        cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                              + ';PORT=1443;DATABASE=' + database
                              + ';UID=' + username + ';PWD=' + password)
        cursor = cnxn.cursor()

        start_time = time.time()
        cursor.execute("select * from quakes where MAG between 0.7 and 8")
        end_time = time.time()

        columns = [column[0] for column in cursor.description]

        results = []
        for row in cursor.fetchall():
            #results.append(row)
            results.append(dict(zip(columns, row)))
            #print(row[0])

        #print(results)
        cursor.close()
        cnxn.close()

        #r.set( cacheName, results)
        #r.get('foo')
        rd.set(cachename, cPickle.dumps(results))

    total_time = end_time - start_time
    return render_template('Redisout.html', ci=results, time=total_time)


# -------- RANDOM QUERY USING REDIS --------
@app.route('/quakerange', methods=['GET'])
def quakerange():
    # connect to DB2
    sql = "select * from quakes".encode('utf-8')
    magn = float(request.args.get('mag'))
    magn1 = float(request.args.get('mag1'))

    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server
                            + ';PORT=1443;DATABASE=' + database
                            + ';UID=' + username + ';PWD=' + password)

    cursor = cnxn.cursor()



    starttime = time.time()
    for i in range(0, 1500):
        random1 = round(random.uniform(float(magn), float(magn1)), 3)
        hash = hashlib.sha224(sql).hexdigest()
        key = "sql_cache:" + hash
        if (rd.get(key)):
            print("This was return from redis")
        else:
            cursor.execute("select * from quakes where mag>'" + str(random1) + "'")
            data = cursor.fetchall()

            rows1 = []
            for x in data:
                rows1.append(str(x))
                rd.set(key, cPickle.dumps(list(rows1)))
        # Put data into cache for 1 hour
                rd.expire(key, 36)
                print ("This is the cached data")
    endtime = time.time()
        # Note that for security reasons we are preparing the statement first,
        # then bind the form input as value to the statement to replace the
        # parameter marker.

    duration = endtime - starttime
    return render_template('index.html', ci=rows1, time=duration)


def hello_world():
    return 'Hello, World!\n This looks just amazing within 5 minutes'


if __name__ == '__main__':
    app.run(debug=True)
