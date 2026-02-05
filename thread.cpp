#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <chrono>
#include <thread>
#include <csignal>


const std::string PYTHON_SCRIPT = "test2.py";
const int RUN_DURATION_SEC = 25;              

int main()
{
    int sessionCount = 1;
    bool jobFinished = false;

    while (!jobFinished)
    {

        pid_t pid = fork();

        if (pid < 0)
        {
            std::cerr << "Fork failed.\n";
            return 1;
        }
        else if (pid == 0)
        {
            execlp("python3", "python3", PYTHON_SCRIPT.c_str(), (char *)NULL);
            std::cerr << "Failed to execute.\n";
            exit(1);
        }
        else
        {
            bool childExitedEarly = false;
            int status;

            for (int i = 0; i < RUN_DURATION_SEC; ++i)
            {
                pid_t result = waitpid(pid, &status, WNOHANG);

                if (result == pid)
                {
                    childExitedEarly = true;
                    break;
                }

                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (childExitedEarly)
            {
                if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0))
                {
                    std::cerr << "Job finished with errors.\n";
                }
                jobFinished = true;
            }
            else
            {

                kill(pid, SIGTERM);
                waitpid(pid, &status, 0);

                std::this_thread::sleep_for(std::chrono::seconds(2));
                sessionCount++;
            }
        }
    }

    return 0;
}