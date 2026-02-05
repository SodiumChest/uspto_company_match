import sys
from pathlib import Path
import os
from rapidfuzz.distance import JaroWinkler

import standarlize
import merge
import match
import manual_check

output_path = Path(__file__).parent / "output"
os.chdir(output_path)

def main(command, args):
    if command=="standarlize":
        if len(args)<3:
            print("usage: python main.py standarlize <file_type> <file_path> <output_path>")
            print(" - file_type: OWNER | ASSIGNEE | ASSIGNOR | CRSP | COMPUSTAT | WRDS | CIQ")
        else:
           standarlize.standarlize_main(args[0], args[1], args[2])

    elif command=="link":
        if len(args)<3:
            print("usage: python main.py link <ciq_path> <linking_path> <output_path>")
        else:
            standarlize.match_ciq_gvkey(args[0],args[1],args[2])

    elif command=="merge":
        if len(args)<3:
            print("usage: python main.py merge <output_path> <type_0> <file_path_0> <type_1> <file_path_1> ...")
            print(" - file_type: CRSP | COMPUSTAT | WRDS | CIQ")
        else:
            merge.merge_main(args)

    elif command=="match":
        if len(args)<4:
            print("usage: python main.py match <source_path> <type> <map_path> <output_path> [temp_path]")
            print(" - file_type: OWNER | ASSIGNEE | ASSIGNOR")
        else:
            match.match_main(
                args[0], args[1], args[2], args[3], None if len(args)<5 else args[4],
                # algorithm=lambda x,y:JaroWinkler.similarity(x,y) * 100,
            )

    elif command=="check":
        if len(args)<3:
            print("usage: python main.py check <file_path> <type> <map_path>")
            print(" - file_type: OWNER | ASSIGNEE | ASSIGNOR")
        else:
            if args[1]=="OWNER":
                manual_check.manual_check_main(
                    args[0],args[2],"own_name",
                )
            elif args[1]=="ASSIGNEE":
                manual_check.manual_check_main(
                    args[0], args[2],"ee_name",
                )
            elif args[1]=="ASSIGNER":
                manual_check.manual_check_main(
                    args[0], args[2],"or_name",
                )
            else:
                print("unknown type: " + args[1])

    elif command=="export":
        if len(args)<4:
            print("usage: python main.py export <file_path> <type> <output_path> <map_path>")
        else:
            if args[1]=="OWNER":
                manual_check.post_process_companies(
                    args[0], args[3], args[2], "serial_no", "own_name"
                )
            elif args[1]=="ASSIGNEE":
                manual_check.post_process_companies(
                    args[0], args[3], args[2], "rf_id", "ee_name"
                )
            elif args[1]=="ASSIGNOR":
                manual_check.post_process_companies(
                    args[0], args[3], args[2], "rf_id", "or_name"
                )
            else:
                print("unknown type: " + args[1])

    elif command=="help" or command=="":
        print("standarlize <file_type> <file_path> <output_path>                        标准化公司名")
        print("link <ciq_path> <linking_path> <output_path>                             链接ciq和gvkey")
        print("merge <output_path> <type_0> <file_path_0> <type_1> <file_path_1> ...    生成公司名：ID映射表")
        print("match <source_path> <type> <map_path> <output_path> [temp_path]          匹配公司名和ID")
        print("check <file_path> <type> <map_path>                                   人工检查match结果")
        print("export <file_path> <type> <output_path> <map_path>                       将check后的结果导出")
    else:
        print("unknown command: "+command)

if __name__ == "__main__":
    try:
        if len(sys.argv)<2:
            main('',[])
        else:
            main(sys.argv[1], sys.argv[2:])
    except Exception as e:
        print(e)