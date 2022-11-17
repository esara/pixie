# Copyright 2018- The Pixie Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

def _graal_native_binary_impl(ctx):
    native_image_binary_path = ctx.toolchains["//bazel/graal:toolchain_type"].graal_native.native_image_binary_path
    cc_compiler_path = ctx.toolchains["@bazel_tools//tools/cpp:toolchain_type"].cc.compiler_executable

    # TODO(james): this is a bit of a hack because bazel's JavaInfo doesn't currently have starlark definitions,
    # so its hard to traverse.
    default_info = ctx.attr.java_binary[DefaultInfo]
    jar = None
    for file in default_info.files.to_list():
        if file.path.endswith(".jar"):
            jar = file
            break
    if jar == None:
        fail("no .jar file in java_binary rule output")

    out_name = ctx.attr.output_name
    if out_name == "":
        out_name = ctx.attr.name

    out = ctx.actions.declare_file(out_name)

    args = [
        "-cp",
        jar.path,
        "-o",
        out.path,
        "--native-compiler-path=" + cc_compiler_path,
        # TODO(james): this should be changed to get all the correct copts/linkopts etc from the CcToolchainInfo.
        "--native-compiler-options=-fuse-ld=lld",
        "--silent",
    ] + ctx.attr.extra_args

    ctx.actions.run(
        outputs = [out],
        inputs = [jar],
        executable = native_image_binary_path,
        arguments = args,
    )

    return [
        DefaultInfo(
            files = depset([out]),
            executable = out,
        ),
    ]

# Caution to user, this rule was designed for a very specific single use case of building a native-image binary from a jar file.
# It is not intended for generalized native-image use.
graal_native_binary = rule(
    implementation = _graal_native_binary_impl,
    attrs = {
        "extra_args": attr.string_list(),
        "java_binary": attr.label(
            providers = [JavaInfo, DefaultInfo],
        ),
        "output_name": attr.string(),
    },
    toolchains = [
        "//bazel/graal:toolchain_type",
        "@bazel_tools//tools/cpp:toolchain_type",
    ],
    executable = True,
)