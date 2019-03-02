set_source_files_properties(enclave_u.c PROPERTIES LANGUAGE CXX )

SET(CMAKE_C_FLAGS "${App_C_Flags}")
SET(CMAKE_CXX_FLAGS "${App_Cpp_Flags}")
SET(App_U_Files "enclave_u.c" "enclave_u.h")
SET_SOURCE_FILES_PROPERTIES(${App_U_Files} PROPERTIES GENERATED TRUE)
INCLUDE_DIRECTORIES(
        ${CMAKE_SOURCE_DIR}/app
        ${CMAKE_SOURCE_DIR}/app/include
        ${CMAKE_SOURCE_DIR}/enclave
        ${CMAKE_SOURCE_DIR}/enclave/include
        ${CMAKE_SOURCE_DIR}/include
)
FILE(GLOB App_H_Files "*.h" "*.hpp" "include/*.h" "include/*.hpp" "Untrusted_LocalAttestation/*.h")
FILE(GLOB App_C_Files "*.c" "Untrusted_LocalAttestation/*.c")
FILE(GLOB App_Cpp_Files "*.cpp" "TrustedLibrary/*.cpp" "Untrusted_LocalAttestation/*.
cpp")
LIST(REMOVE_ITEM App_H_Files "${CMAKE_CURRENT_SOURCE_DIR}/enclave_u.h")
LIST(REMOVE_ITEM App_C_Files "${CMAKE_CURRENT_SOURCE_DIR}/enclave_u.c")
LIST(REMOVE_ITEM App_Cpp_Files "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
ADD_CUSTOM_COMMAND(OUTPUT ${App_U_Files}
        COMMAND "${SGX_EDGER8R}" --untrusted ${CMAKE_SOURCE_DIR}/enclave/enclave.edl --search-path "${CMAKE_SOURCE_DIR}/enclave" --search-path "${SGX_SDK}/include"
        COMMAND cp ${App_U_Files} "${CMAKE_SOURCE_DIR}/app"
        COMMAND "echo" 'GEN => ${CMAKE_SOURCE_DIR}/app/enclave_u.c'
        )
ADD_LIBRARY(App_lib ${App_Cpp_Files} ${App_C_Files} ${App_U_Files} ${App_H_Files})
ADD_EXECUTABLE(${App_Name} "main.cpp" "${App_U_Files}" "${App_Cpp_Files}" "${App_C_Files}" "${App_H_Files}")
TARGET_LINK_LIBRARIES(${App_Name} ${App_Link_Flags} ${App_Cpp_Flags})
