// swift-tools-version: 5.7
import PackageDescription

let package = Package(
    name: "MacosNativeMenuSwift",
    platforms: [
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "MacosNativeMenuSwift",
            type: .static,
            targets: ["MacosNativeMenuSwift"]
        ),
    ],
    targets: [
        .target(
            name: "MacosNativeMenuSwift",
            exclude: [
                "Resources",
            ]
        ),
    ]
)
