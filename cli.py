import os
import subprocess
from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory


def help(args):
    #print('This is hello command.')
    if args == []:
        print("1. ls\t\tList all the file and folders.\n\t-l\tList all teh file and folders in a detailed manner.\n\t-al\tList the hidden files along wiht remaining files.")
        print("2. rm [path]\tRemove the files in the given path.")
        print("7. quit/exit\tTo stop the execution of the filesystem.")
    else:
        print(
            f"'help {' '.join(args)}' command not found\nEnter 'help' to know more..")


def cat():
    print('This is cat command.')


def put():
    print('This is put command.')


def ls(args):
    #print('This is ls command.')
    temp = 0
    k = 0
    if args != []:
        k = 1
        temp = subprocess.Popen(['ls', args[0]], stdout=subprocess.PIPE)
    else:
        temp = subprocess.Popen('ls', stdout=subprocess.PIPE)
    output = str(temp.communicate())
    output = output.split('\'')
    output = output[1].split('\\')
    res = []
    i = 0
    for line in output:
        if k == 1:
            if i != 0:
                res.append(line[1:])
            else:
                res.append(line)
            i += 1
        else:
            if i == 0:
                res.append(line)
            elif i < len(output)-1:
                res.append(line[1:])
            i += 1
    for i in res:
        print(i)
    return 0


def rm(args):
    #print('This is rm command.')
    for i in args:
        if os.path.isfile(i):
            os.remove(i)
        elif os.path.isdir(i):
            print(f"rm: cannot remove '{i}': Is a directory")
        else:
            print(f"rm: cannot remove '{i}': No such file or directory")


def rmdir():
    print('This is rmdir command.')


def mkdir():
    print('This is mkdir command.')


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
    try:
        val = fun(args)
    except Exception as e:
        return e
    else:
        return val


while 1:
    user_input = prompt('>')

    try:
        userInputList = user_input.split(' ')
        command = comm[userInputList[0]]
        val = err(command, userInputList[1:])
        if val == -1:
            break
    except Exception as e:
        print(f"{e} is not a proper command\nEnter proper command!!")
