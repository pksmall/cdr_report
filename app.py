import datetime
import os
import mysql.connector as MySQL
import sys
import getopt
from calendar import monthrange

EXT_FILE = "data/ext.txt"


def main(argv):
  today = datetime.datetime.today()
  year = today.year
  month = today.month
  try:
    opts, args = getopt.getopt(argv, "hy:m:")
  except getopt.GetoptError:
    sys.exit(-2)
  for opt, arg in opts:
    if opt == '-h':
      print(sys.argv[0] + " -y year -m month ")
      print("example: " + sys.argv[0] + " -y 2020 -m 3")
      print("get cdr report from March 2020")
      sys.exit(1)
    elif opt in "-y":
      year = int(arg)
    elif opt in "-m":
      month = int(arg)

  m_range = monthrange(year, month)[1] + 1
  print(m_range)

  print("Current year and month: " + str(year) + " " + str(month))
  file_line_list = []
  try:
    file_line_list = [line.rstrip('\n') for line in open(os.path.join("./", EXT_FILE), 'r')]
  except Exception as ex:
    print(ex)
    exit(-1)

  print(file_line_list)
  print("")
  # Open database connection
  db = MySQL.connect(user='root ', password='root', host='localhost', database='calldb')

  # prepare a cursor object using cursor() method
  cursor = db.cursor()

  for ext_number in file_line_list:
    print(ext_number)
    for day_idx in range(1, m_range):
      # посчитаем все отвеченные звонки
      query = "SELECT count(*) as count " \
              "FROM cdr WHERE calldate BETWEEN cast(%s as DATETIME) " \
              "and  cast(%s as DATETIME) and disposition = 'ANSWERED' and dst = %s"
      # вывести список всех звонков отвеченых
      """
      query = "SELECT calldate, src, dst, duration, disposition " \
              "FROM cdr WHERE calldate BETWEEN cast(%s as DATETIME) " \
              "and  cast(%s as DATETIME) and disposition = 'ANSWERED' and dst = %s"
      """
      from_date = datetime.datetime(year, month, day_idx, 00, 00, 00)
      to_date = datetime.datetime(year, month, day_idx, 23, 59, 59)
      # execute SQL query using execute() method.
      cursor.execute(query, (from_date, to_date, ext_number))

      # отобразим количество звонков, с нулем игнорируем
      for (count) in cursor:
        if count[0] > 0:
          print(str(year) + "/" + str(month) + "/" + str(day_idx) + " = " + str(count[0]))
      # отображаем все звонки
      """
      for (calldate, src, dst, duration, disposition) in cursor:
        print("{} {} {} {} {}".format(calldate, src, dst, duration, disposition))
      if cursor.rowcount > 0:
        print("#"*20)
      """

  cursor.close()
  # disconnect from server
  db.close()


if __name__ == '__main__':
  main(sys.argv[1:])
