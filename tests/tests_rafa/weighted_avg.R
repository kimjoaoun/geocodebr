I have two tables:

  tableA looks like this:

  id | number
1 | 100
2 | 120
3 | 300

table B looks like this:
  number | longitude
100 | -41.1
100 | -41.101
150 | -41.3
300 | -41.2

Write the code in SQL with duckdb to perform left join between tables A and B based on column 'id' so that the logitude of an 'id' will be the average of longitudes in table B weighted by the difference between the numbers in table A and table B




"SELECT
    A.id,
    SUM((1/ABS(A.number - B.number) * B.longitude)) / SUM(1/ABS(A.number - B.number)) AS weighted_avg_longitude
FROM
    tableA AS A
LEFT JOIN
    tableB AS B
ON
    A.number = B.number
GROUP BY
    A.id;"
