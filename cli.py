import os
import subprocess
from prompt_toolkit import output, prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
import shutil


def help(args):

    if args == []:
        print("1. ls\t\tList all the file and folders.\n\t-l\tList all the file and folders in a detailed manner.\n\t-al\tList the hidden files along wiht remaining files.")
        print("2. cat [filename]\tDisplay the contents of the file.")
        print("2. rm [filename]\tRemove the files in the given path.")
        print("3. rmdir [filename]\tRemove the specified directory.")
        print("4. mkdir [filename]\tCreate directory in the specified path.")
        print("7. quit/exit\tTo stop the execution of the filesystem.")
    else:
        print(
            f"'help {' '.join(args)}' command not found\nEnter 'help' to know more..")


def cat(args):

    if args == []:
        print("cat: no arguments specified")
        return
    for arg in args:
        temp = subprocess.Popen(['cat', arg], stdout=subprocess.PIPE)
        try:
            output = str(temp.communicate())
            output = output.split('\'')
            output = output[1].split('\\n')
            for i in output:
                print(i)
        except Exception as e:
            print(e)



def put(args):

    if len(args)==0:
        print("put: no arguments specified")
        return
    elif len(args)==1:
        print("put: no target dir mentioned")
        return
        
    locs,loc2 = args[:-1],args[-1]
    locs = [i.replace('\\','/') for i in locs]
    loc2 = loc2.replace('\\','/')

    for loc in locs:
        try:
            shutil.copy(loc,loc2)
        except Exception as e: print(e)



def ls(args):

    temp = 0
    if args != []:
        temp = subprocess.Popen(['ls', args[0]], stdout=subprocess.PIPE)
    else:
        temp = subprocess.Popen('ls', stdout=subprocess.PIPE)
    output = str(temp.communicate())
    output = output.split('\'')
    output = output[1].split('\\n')
    for line in output:
        if os.path.isdir(line):
            line = "./"+line
        print(line)


def rm(args):
    if args == []:
        print("rm: specify name(s) of file(s) to remove")
        return
    for i in args:
        if os.path.isfile(i):
            os.remove(i)
        elif os.path.isdir(i):
            print(f"rm: cannot remove '{i}': Is a directory")
        else:
            print(f"rm: cannot remove '{i}': No such file or directory")


def rmdir(args):
    if args == []:
        print("rmdir: specify name(s) of folder(s) to remove")
        return
    for i in args:
        if os.path.isdir(i):
            os.rmdir(i)
        elif os.path.isfile(i):
            print(f"rmdir: failed to remove '{i}': Not a directory")
        else:
            print(f"rmdir: failed to remove '{i}': No such file or directory")


def mkdir(args):
    if args == []:
        print("mkdir: specify name(s) of folder(s) to create")
        return
    for i in args:
        try:
            os.mkdir(i)
        except:
            print(f"mkdir: cannot create directory '{i}': File exists")


def quit(args):
    if args == []:
        print('\nGoodbye!!')
        return -1
    else:
        print(
            f"'quit {' '.join(args)}' not found!!\n Try 'quit' to quit the fs")


def exit(args):
    if args == []:
        print('\nGoodbye!!')
        return -1
    else:
        print(
            f"'exit {' '.join(args)}' not found!!\n Try 'exit' to quit the fs")


comm = {
    'help': help,
    'cat': cat,
    'put': put,
    'ls': ls,
    'rm': rm,
    'rmdir': rmdir,
    'mkdir': mkdir,
    'quit': quit,
    'exit': exit
}


def err(fun, args):
    try :
        val = fun(args)
    except Exception as e:
        return e
    else :
        return val


while 1:
    user_input = prompt('>',auto_suggest=AutoSuggestFromHistory(),history=FileHistory('./logs/history.txt'))

    try:
        userInputList = user_input.split(' ')
        command = comm[userInputList[0]]
        val = err(command, userInputList[1:])
        if val == -1:
            break
    except Exception as e:
        print(f"{e} is not a proper command\nEnter proper command!!")
