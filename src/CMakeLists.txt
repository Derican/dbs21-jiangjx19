# -*- mode:cmake -*-
if(NOT UNIX)
  message(WARNING "Unsupported operating system")
endif()

set(antlr4-src-GENERATED_SRC
   ${PROJECT_SOURCE_DIR}/src/generated/SQLBaseListener.cpp 
   ${PROJECT_SOURCE_DIR}/src/generated/SQLBaseVisitor.cpp
   ${PROJECT_SOURCE_DIR}/src/generated/SQLLexer.cpp
   ${PROJECT_SOURCE_DIR}/src/generated/SQLListener.cpp
   ${PROJECT_SOURCE_DIR}/src/generated/SQLParser.cpp
   ${PROJECT_SOURCE_DIR}/src/generated/SQLVisitor.cpp
 )

foreach(src_file ${antlr4-src-GENERATED_SRC})
      set_source_files_properties(
          ${src_file}
          PROPERTIES
          GENERATED TRUE
          )
endforeach(src_file ${antlr4-src-GENERATED_SRC})

add_custom_target(GenerateParser DEPENDS ${antlr4-src-GENERATED_SRC})
add_custom_command(OUTPUT ${antlr4-src-GENERATED_SRC}
   COMMAND 
   ${CMAKE_COMMAND} -E make_directory ${PROJECT_SOURCE_DIR}/src/generated/
   COMMAND
   "${Java_JAVA_EXECUTABLE}" -jar ${ANTLR_JAR_LOCATION} -Werror -Dlanguage=Cpp -listener -visitor -o ${PROJECT_SOURCE_DIR}/src/generated/ ${PROJECT_SOURCE_DIR}/src/SQL.g4
   WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
   DEPENDS ${PROJECT_SOURCE_DIR}/src/SQL.g4
   )

include_directories(
  ${PROJECT_SOURCE_DIR}/runtime/src
  ${PROJECT_SOURCE_DIR}/runtime/src/misc
  ${PROJECT_SOURCE_DIR}/runtime/src/atn
  ${PROJECT_SOURCE_DIR}/runtime/src/dfa
  ${PROJECT_SOURCE_DIR}/runtime/src/tree
  ${PROJECT_SOURCE_DIR}/runtime/src/support
  ${PROJECT_SOURCE_DIR}/src/generated
  ${PROJECT_SOURCE_DIR}/src/bufmanager
  ${PROJECT_SOURCE_DIR}/src/fileio
  ${PROJECT_SOURCE_DIR}/src/utils
  ${PROJECT_SOURCE_DIR}/src/recmanager
  ${PROJECT_SOURCE_DIR}/src/ixmanager
  ${PROJECT_SOURCE_DIR}/src/sysmanager
  ${PROJECT_SOURCE_DIR}/src/qlmanager
  )

#file(GLOB antlr4-src_SRC "${PROJECT_SOURCE_DIR}/src/generated/*")
set(antlr4-src_SRC 
  ${PROJECT_SOURCE_DIR}/src/main.cpp
  ${PROJECT_SOURCE_DIR}/src/MinisqlVisitor.hpp
  ${antlr4-src-GENERATED_SRC}
  )

if(NOT CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
  set (flags_1 "-Wno-overloaded-virtual")
else()
  set (flags_1 "-MP /wd4251")
endif()

foreach(src_file ${antlr4-src_SRC})
      set_source_files_properties(
          ${src_file}
          PROPERTIES
          COMPILE_FLAGS "${COMPILE_FLAGS} ${flags_1}"
          )
endforeach(src_file ${antlr4-src_SRC})

add_executable(minisql
  ${antlr4-src_SRC}
  )
#add_precompiled_header(minisql ${PROJECT_SOURCE_DIR}/runtime/src/antlrcpp-Prefix.h)

if(CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
  target_compile_options(minisql PRIVATE "/MT$<$<CONFIG:Debug>:d>")
endif()

add_dependencies(minisql GenerateParser)

target_link_libraries(minisql antlr4_static)

install(TARGETS minisql 
        DESTINATION "share" 
        COMPONENT dev 
        )

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/run)

