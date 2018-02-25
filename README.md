# docker-hadoop-client

## 概要

[docker-hadoop-server](https://github.com/jozuo/docker-hadoop-server)で構築したHadoopの完全分散モード環境を利用するプログラムの開発用環境。  
`Spark`/`Hive`/`HBase`が利用できる想定。  
`./program/`下に配置したプログラムが、コンテナ上も同じパスにマウントされるようにしています。


## 起動方法
### 前準備

`.env`ファイルを以下の内容で作成する。

```
WORKSPACE_DIR=[cloneしたディレクトリ]
```

### 毎回

```
docker-compose up -d
```
