# 6.824
2021 6.824

## 如何测试

要求python3的环境，测试脚本是实验材料中给的python脚本。
```sh
    cd 6.824/raft
    # -v 打印输出信息
    # -p 最大并行数
    # -n 测试次数
    # -o 错误结果保存目录
    # [Tests]指定的测试函数列表
    python3 dstest.py -v  -p 20 -n 100 -o ./output [Tests]
```

查看错误日志
```
    cd 6.824/raft
    # -c 输出的列数，即测试中servers的数目
    python3 dslog.py output/xxx.log -c 3
```
dstest.py，dslog.py具体使用方式见：[Debugging by Pretty Printing (josejg.com)](https://blog.josejg.com/debugging-pretty/)，本文做了少量修改，但使用方式未变。

## Lab 2的实现见`/docs`内的文档


