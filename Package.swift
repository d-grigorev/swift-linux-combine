// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "LinuxCombine",
    products: [
        .library(name: "LinuxCombine", targets: ["LinuxCombine"]),
        .executable(name: "LinuxCombineBench", targets: ["LinuxCombineBench"]),
    ],
    dependencies: [
        // Baseline for the performance challenge — pinned to the exact
        // commit the borrowed parity tests come from.
        .package(
            url: "https://github.com/OpenCombine/OpenCombine.git",
            revision: "1c6f02c7ed8140c0ba7a783aaddb6e0685a0037b"
        ),
    ],
    targets: [
        .target(name: "LinuxCombine"),
        .testTarget(name: "LinuxCombineTests", dependencies: ["LinuxCombine"]),
        // Every code example in docs/*.md is compiled and executed here
        // (marker-per-example; Tools/check_doc_examples.py enforces the
        // mapping), so the documentation cannot drift from behavior.
        .testTarget(name: "DocumentationExamplesTests", dependencies: ["LinuxCombine"]),
        .testTarget(
            name: "OpenCombineParityTests",
            dependencies: ["LinuxCombine"],
            exclude: ["README.md", "LICENSE-OpenCombine"]
        ),
        .executableTarget(
            name: "LinuxCombineBench",
            dependencies: [
                "LinuxCombine",
                .product(name: "OpenCombine", package: "OpenCombine"),
            ]
        ),
    ],
    swiftLanguageModes: [.v5]
)
