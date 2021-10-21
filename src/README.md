## Minisql application for the ANTLR 4 C++ target

This app shows the minisql project offering simple dabatases operations.

A few steps are necessary to get this to work:

- Download the current ANTLR jar and place it in the root folder.
- Run `java -jar ../antlr-4.9.2-complete.jar -DLanguage=Cpp -listener -no-visiter -o generated/` to generate the parse code.
- Compile and run.

Compilation is done as described in the [dbs21-jiangjx19/readme.md](../README.md) file.
