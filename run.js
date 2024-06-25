import {ACEBuffer} from "https://emblaisdell.github.io/ontolog/ace/types.js"

const { loadPyodide } = await import('https://cdn.jsdelivr.net/pyodide/v0.23.2/full/pyodide.mjs');

const pyodide = await loadPyodide();
pyodide.runPython("import pickle")

export async function runRaw(bPyCode, bLibraries, bFuncName, bArgs) {
    const pyCode = bPyCode.toString()
    const libraries = bLibraries.toList().map(bLib => bLib.toString())
    const funcName = bFuncName.toString()
    const pArgs = bArgs.toList().map(arg => arg.uint8Array)

    const output = await run(pyCode, libraries, funcName, pArgs)

    return pyodide.runPython("lambda x: pickle.dumps(x)")(output).toJs()
}

async function run(pyCode, libraries, funcName, pArgs) {
    await pyodide.loadPackage(libraries)
    await pyodide.runPythonAsync(pyCode)

    // console.log("pArgs", pArgs)

    const args = pArgs.map(arg => pyodide.runPython("lambda b: pickle.loads(bytes(b))")(arg))

    // console.log("args", args)

    return pyodide.globals.get(funcName)(...args)
}

function pickleString(bStr) {
    return pyodide.runPython("lambda s: pickle.dumps(s)")(bStr.toString()).toJs()
}

function unpickleString(pStr) {
    return ACEBuffer.fromString(pyodide.runPython("lambda b: pickle.loads(bytes(b))")(pStr))
}

const pHello = new ACEBuffer(pickleString(ACEBuffer.fromString("hello")))

const runRawOutput = await runRaw(ACEBuffer.fromString(`
    def my_concat(a,b):
        return a+b
    `), ACEBuffer.fromList([]), "my_concat", ACEBuffer.fromList([pHello,pHello]))

console.log("runRawOutput", runRawOutput)
console.log("runRawOutput unpickled", unpickleString(runRawOutput))
