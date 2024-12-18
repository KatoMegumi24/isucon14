#!/bin/bash -eux

# ../$(hostname)/deploy.sh があればそちらを実行して終了
if [ -e ../$(hostname)/deploy.sh ]; then
  ../$(hostname)/deploy.sh
  exit 0
fi

# 各種設定ファイルのコピー
# ../$(hostname)/env.sh があればそちらを優先してコピーする
if [ -e ../$(hostname)/env.sh ]; then
  sudo cp -f ../$(hostname)/env.sh /home/isucon/env.sh
else
  sudo cp -f env.sh /home/isucon/env.sh
fi

# etc以下のファイルについてすべてコピーする
for file in `\find etc -type f`; do
  # .gitkeepはコピーしない
  if [ $file = "etc/.gitkeep" ]; then
    continue
  fi

  # 同名のファイルが ../$(hostname)/etc/ にあればそちらを優先してコピーする
  if [ -e ../$(hostname)/$file ]; then
    sudo cp -f ../$(hostname)/$file /$file
    continue
  fi
  sudo cp -f $file /$file
done

# アプリケーションのビルド
APP_NAME=isuride
# ディレクトリ構成確認して必要なら書き換える
cd /home/isucon/webapp/go/

# もしpgo.pb.gzがあればPGOを利用してビルド
if [ -e pgo.pb.gz ]; then
  go build -o ${APP_NAME} -pgo=pgo.pb.gz
else
  go build -o ${APP_NAME}
fi


# ミドルウェア・Appの再起動
sudo systemctl restart mysql
sudo systemctl reload nginx
sudo systemctl restart ${APP_NAME}-go
sudo systemctl restart isuride-matcher

# slow query logの有効化
QUERY="
set global slow_query_log_file = '/var/log/mysql/mysql-slow.log';
set global long_query_time = 0;
set global slow_query_log = ON;
"
echo $QUERY | sudo mysql -uroot

# log permission
sudo chmod -R 777 /var/log/nginx
sudo chmod -R 777 /var/log/mysql
