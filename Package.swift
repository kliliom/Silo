// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Silo",
    platforms: [
        .iOS(.v17),
        .macCatalyst(.v17),
        .macOS(.v14),
        .tvOS(.v17),
        .visionOS(.v1),
        .watchOS(.v10)
    ],
    products: [
        .library(
            name: "Silo",
            targets: ["Silo"]),
    ],
    targets: [
        .target(
            name: "Silo",
            dependencies: []),
        .testTarget(
            name: "SiloTests",
            dependencies: ["Silo"]),
    ]
)

if Context.environment["BUILDING_DOCC"] != nil {
    package.dependencies.append(
        .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
    )
}