from prompt_toolkit import prompt
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory


def err(fun):
    try:
        val = fun()
    except Exception as e:
        return e
    else:
        return val

def hello():
    print('This is hello command.')

def cat():
    print('This is cat command.')

def put():
    print('This is put command.')

def ls():
    print('This is ls command.')

def rm():
    print('This is rm command.')

def rmdir():
    print('This is rmdir command.')

def mkdir():
    print('This is mkdir command.')

def quit():
    return -1


comm = {
    'hello': hello,
    'cat': cat,
    'put': put,
    'ls': ls,
    'rm': rm,
    'rmdir': rmdir,
    'mkdir': mkdir,
    'quit': quit
}





while 1:
    user_input = prompt('>',
                        history=FileHistory('history.txt'),
                        auto_suggest=AutoSuggestFromHistory(),
                        )

    try:
        command = comm[user_input]
        s = command()
        if s == -1:
            break
    except Exception as e:
        print(f"{e}: Unknown command")




# if __name__ == '__main__':

#     while 1:
#         user_input = prompt('>',
#                             history=FileHistory('history.txt'),
#                             auto_suggest=AutoSuggestFromHistory(),
#                         )
#         command = comm[user_input]
#         command()
#         print(user_input)
        





# from cmd import Cmd
 
# class MyPrompt(Cmd):
#    def do_exit(self, inp):
#         print("Bye")
#         return True
 
#    def do_add(self, inp):
#         print(type(inp))
#         print("Adding '{}'".format(inp))
 
# MyPrompt().cmdloop()
# print("after")
