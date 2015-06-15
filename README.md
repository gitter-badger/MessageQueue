# MessageQueue
MessageQueue is a simple multi-thread driver to call APIs, getting the data from MySQL table.

Features

- All threads uses Keep-Alive connection to keep everything faster.. So it does not need to connect/disconnect on every connect.
- This driver do a while(true) doing SELECTs no MySQL, then start many threads that call those APIs and finally, it update all record on MySQL at once... Avoiding UPDATE on each row.
