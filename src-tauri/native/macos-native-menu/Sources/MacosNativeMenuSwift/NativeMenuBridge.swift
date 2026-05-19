import AppKit
import CoreFoundation
import Foundation

@_silgen_name("macos_native_menu_dispatch_action")
private func rustDispatchMenuAction(
    _ action: UnsafePointer<CChar>?,
    _ platformId: UnsafePointer<CChar>?,
    _ accountId: UnsafePointer<CChar>?
)

private func runNativeMenuControllerSync(
    label: String,
    _ operation: @escaping () -> Void
) {
    precondition(Thread.isMainThread, "[NativeMenu] \(label) 必须在主线程执行")
    operation()
}

private let nativeMenuRunLoopModes: [RunLoop.Mode] = [
    .default,
    .eventTracking,
    .modalPanel,
]

private func runNativeMenuController(
    label: String,
    _ operation: @escaping () -> Void
) {
    if Thread.isMainThread {
        runNativeMenuControllerSync(label: label, operation)
        return
    }

    RunLoop.main.perform(inModes: nativeMenuRunLoopModes) {
        runNativeMenuControllerSync(label: label, operation)
    }
    CFRunLoopWakeUp(CFRunLoopGetMain())
}

func dispatchRustMenuAction(action: String, platformId: String? = nil, accountId: String? = nil) {
    action.withCString { actionPointer in
        if let platformId {
            platformId.withCString { platformPointer in
                if let accountId {
                    accountId.withCString { accountPointer in
                        rustDispatchMenuAction(actionPointer, platformPointer, accountPointer)
                    }
                } else {
                    rustDispatchMenuAction(actionPointer, platformPointer, nil)
                }
            }
        } else {
            rustDispatchMenuAction(actionPointer, nil, nil)
        }
    }
}

@_cdecl("macos_native_menu_toggle")
public func macos_native_menu_toggle(
    snapshotJSONPointer: UnsafePointer<CChar>?,
    statusItemPointer: UnsafeMutableRawPointer?
) {
    guard let snapshotJSONPointer, let statusItemPointer else { return }
    let snapshotJSON = String(cString: snapshotJSONPointer)
    Task { @MainActor in
        NativeMenuPopoverController.shared.toggle(
            snapshotJSON: snapshotJSON,
            statusItemPointer: statusItemPointer
        )
    }
}

@_cdecl("macos_native_menu_update_snapshot")
public func macos_native_menu_update_snapshot(
    snapshotJSONPointer: UnsafePointer<CChar>?
) {
    guard let snapshotJSONPointer else { return }
    let snapshotJSON = String(cString: snapshotJSONPointer)
    Task { @MainActor in
        NativeMenuPopoverController.shared.update(snapshotJSON: snapshotJSON)
    }
}
