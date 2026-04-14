#!/bin/sh

printf "Enter timestamp: "
read number

sqlite3 <<EOS
.mode column
.width 12, 20

select 'Number:',     $number;
select 'Unix epoch:', datetime($number, 'unixepoch');
select 'Variant:',    datetime($number, 'unixepoch', '-70 years');
select 'Julian day:', datetime($number);
select 'Mac HFS+:',   datetime($number, 'unixepoch', '-66 years');
select 'Apple CoreD:';
select '  (seconds)', datetime($number, 'unixepoch', '+31 years');
select '  (nanosec)', datetime($number/1000000000, 'unixepoch', '+31 years');
select 'NET:',        datetime($number/10000000, 'unixepoch', '-1969 years');
EOS
