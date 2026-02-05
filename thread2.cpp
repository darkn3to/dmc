#include <iostream>
#include <fstream>
#include <cstdlib>
#include <unistd.h>
#include <sys/wait.h>

// Function to check if a file exists
bool file_exists(const std::string& name) {
    std::ifstream f(name.c_str());
    return f.good();
}

int main() {
    // Configuration
    std::string pythonScript = "test2.py";
    std::string checkpointFile = "checkpoint.pth";

    std::cout << "[Machine B] Preparing to resume process...\n";

    // 1. Sanity Check: Does the checkpoint exist?
    if (!file_exists(checkpointFile)) {
        std::cerr << "[Error] '" << checkpointFile << "' not found!\n";
        std::cerr << ">> Please transfer the checkpoint file from Machine A to this folder.\n";
        return 1;
    }

    std::cout << "[Machine B] Checkpoint found. Launching Python script...\n";
    std::cout << "------------------------------------------------------\n";

    // 2. Fork and Execute
    pid_t pid = fork();

    if (pid < 0) {
        std::cerr << "[Error] Fork failed.\n";
        return 1;
    } else if (pid == 0) {
        // --- CHILD PROCESS ---
        // Run the python script. It will automatically detect the checkpoint and resume.
        execlp("python3", "python3", pythonScript.c_str(), (char*)NULL);
        
        // If we get here, execlp failed
        std::cerr << "[Error] Failed to run python3.\n";
        exit(1);
    } else {
        // --- PARENT PROCESS ---
        int status;
        
        // Wait for the python script to finish execution
        waitpid(pid, &status, 0);
        
        if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
            std::cout << "\n[Machine B] Process completed successfully.\n";
        } else {
            std::cout << "\n[Machine B] Process finished with potential errors.\n";
        }
    }

    return 0;
}