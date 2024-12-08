# isucon-app-scaffold

## 使い方

```bash
# デプロイ
make deploy BRNACH=master
make deploy-pgo BRNACH=master
```

## 初動

1. テンプレートリポジトリからリポジトリを作成
2. デプロイキーを設定 (複数リポジトリで同じデプロイキーを使用できない)
3. ansible の変数に今回使うリポジトリを書き込む
4. ansible を回す
5. リポジトリに内容をぶち込む
6. common/env.shに~/env.shの内容をコピーする
7. common/deploy.shの必要箇所を修正
   - 33行目: `APP_NAME` を変更
   - 35行目: 必要ならディレクトリを変更
8. repo/app以下のシンボリックリンクを~/webapp/に作成
   - repositoryに加えた変更を/goと/sqlに反映するため
9. Start!

## 雑メモ

```
sudo journalctl -e -u $(SERVICE_NAME)
systemctl status app
mysql -h$(DB_HOST) -P$(DB_PORT) -u$(DB_USER) -p$(DB_PASS) -t -e 'USE information_schema; SELECT table_name, column_name FROM columns WHERE table_schema="$(DB_NAME)";' >> /temp/status.txt
mysql -h$(DB_HOST) -P$(DB_PORT) -u$(DB_USER) -p$(DB_PASS) $(DB_NAME)
```

```
ulimit -l 10000
ulimit -n 1006500
sudo nginx -t
```
