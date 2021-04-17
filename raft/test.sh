#!/usr/bin/env bash



# -p 并发数
# -n 执行次数
# -v 打印输出
python3 dstest.py -v  -p 10 -n 50 -o ./output
InitialElection2A
TestReElection2A
TestManyElections2A
BasicAgree2B
RPCBytes2B
FailAgree2B
FailNoAgree2B
ConcurrentStarts2B
Rejoin2B
Backup2B
Count2B
TestPersist12C
TestPersist22C
TestPersist32C
TestFigure82C
TestUnreliableAgree2C
TestFigure8Unreliable2C
TestReliableChurn2C
TestUnreliableChurn2C
TestSnapshotBasic2D
TestSnapshotInstall2D
TestSnapshotInstallUnreliable2D
TestSnapshotInstallCrash2D
TestSnapshotInstallUnCrash2D