package apoc.custom;

import apoc.util.JsonUtil;
import org.antlr.v4.runtime.*;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.procedure.Mode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;

public class Signatures {

    public SignatureParser.ProcedureContext parseProcedure(String procedureSignatureText) {
        return parse(procedureSignatureText).procedure();
    }

    public SignatureParser.FunctionContext parseFunction(String functionSignatureText) {
        return parse(functionSignatureText).function();
    }

    public SignatureParser parse(String signatureText) {
        CodePointCharStream inputStream = CharStreams.fromString(signatureText);
        SignatureLexer signatureLexer = new SignatureLexer(inputStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(signatureLexer);
        List<String> errors = new ArrayList<>();
        SignatureParser signatureParser = new SignatureParser(commonTokenStream);
        signatureParser.addErrorListener(new BaseErrorListener() {
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                errors.add("line " + line + ":" + charPositionInLine + " " + msg);
            }
        });
        if (signatureParser.getNumberOfSyntaxErrors() > 0)
            throw new RuntimeException("Syntax Error in " + signatureText + " : " + errors.toString());
        return signatureParser;
    }

    public ProcedureSignature toProcedureSignature(SignatureParser.ProcedureContext signature) {
        QualifiedName name = new QualifiedName(signature.namespace().name().stream().map(this::name).collect(Collectors.toList()), name(signature.name()));

        if (signature.results() == null) {
            System.out.println("signature = " + signature);
            return null;
        }
        List<FieldSignature> outputSignature =
                signature.results().empty() != null ? Collections.emptyList() :
                        signature.results().result().stream().map(p ->
                                FieldSignature.outputField(name(p.name()), type(p.type()))).collect(Collectors.toList());
        // todo deprecated + default value
        List<FieldSignature> inputSignatures = signature.parameter().stream().map(p -> FieldSignature.inputField(name(p.name()), type(p.type()), defaultValue(p.defaultValue(), type(p.type())))).collect(Collectors.toList());
        Mode mode = Mode.DEFAULT;
        boolean admin = false;
        String deprecated = "";
        String[] allowed = null;
        String description = "todo description";
        String warning = null; // "todo warning";
        boolean eager = false;
        boolean caseInsensitive = true;
        return new ProcedureSignature(name, inputSignatures, outputSignature, mode, admin, deprecated, allowed, description, warning, eager, caseInsensitive);
    }

    public UserFunctionSignature toFunctionSignature(SignatureParser.FunctionContext signature) {
        QualifiedName name = new QualifiedName(signature.namespace().name().stream().map(this::name).collect(Collectors.toList()), name(signature.name()));

        if (signature.type() == null) {
            System.out.println("signature = " + signature);
            return null;
        }

        Neo4jTypes.AnyType type = type(signature.type());

        List<FieldSignature> inputSignatures = signature.parameter().stream().map(p -> FieldSignature.inputField(name(p.name()), type(p.type()), defaultValue(p.defaultValue(), type(p.type())))).collect(Collectors.toList());

        String deprecated = "";
        String[] allowed = null;
        String description = "todo description";
        boolean caseInsensitive = true;
        return new UserFunctionSignature(name, inputSignatures, type, deprecated, allowed, description, caseInsensitive);
    }

    private DefaultParameterValue defaultValue(SignatureParser.DefaultValueContext defaultValue, Neo4jTypes.AnyType type) {
        if (defaultValue == null) return DefaultParameterValue.nullValue(type);
        SignatureParser.ValueContext v = defaultValue.value();
        if (v.nullValue() != null)
            return DefaultParameterValue.nullValue(type);
        if (v.boolValue() != null)
            return DefaultParameterValue.ntBoolean(Boolean.parseBoolean(v.boolValue().getText()));
        if (v.stringValue() != null)
            return DefaultParameterValue.ntString(v.stringValue().getText());
        if (v.INT_VALUE() != null)
            return DefaultParameterValue.ntInteger(Integer.parseInt(v.INT_VALUE().getText()));
        if (v.FLOAT_VALUE() != null)
            return DefaultParameterValue.ntFloat(Integer.parseInt(v.FLOAT_VALUE().getText()));
        if (v.mapValue() != null) {
            Map map = JsonUtil.parse(v.mapValue().getText(), null, Map.class);
            return DefaultParameterValue.ntMap(map);
        }
        if (v.listValue() != null) {
            List<?> list = JsonUtil.parse(v.listValue().getText(), null, List.class);
            return DefaultParameterValue.ntList(list, ((Neo4jTypes.ListType) type).innerType());
        }
        return DefaultParameterValue.nullValue(type);
    }

    public String name(SignatureParser.NameContext ns) {
        if (ns.IDENTIFIER() != null) return ns.IDENTIFIER().getText();
        if (ns.QUOTED_IDENTIFIER() != null) return ns.QUOTED_IDENTIFIER().getText(); // todo
        throw new IllegalStateException("Invalid Name " + ns);
    }

    private Neo4jTypes.AnyType type(SignatureParser.TypeContext typeContext) {
        if (typeContext.list_type() != null) {
            return Neo4jTypes.NTList(type(typeContext.list_type().opt_type()));
        }
        if (typeContext.opt_type() != null) {
            return type(typeContext.opt_type());
        }
        return Neo4jTypes.NTAny;
    }

    private Neo4jTypes.AnyType type(SignatureParser.Opt_typeContext opt_type) {
        switch (opt_type.base_type().getText()) {
            case "ANY":
                return NTAny;
            case "MAP":
                return NTMap;
            case "NODE":
                return NTNode;
            case "REL":
                return NTRelationship;
            case "RELATIONSHIP":
                return NTRelationship;
            case "EDGE":
                return NTRelationship;
            case "PATH":
                return NTPath;
            case "NUMBER":
                return NTNumber;
            case "LONG":
                return NTInteger;
            case "INT":
                return NTInteger;
            case "INTEGER":
                return NTInteger;
            case "FLOAT":
                return NTFloat;
            case "DOUBLE":
                return NTFloat;
            case "BOOL":
                return NTBoolean;
            case "BOOLEAN":
                return NTBoolean;
            case "DATE":
                return NTDate;
            case "TIME":
                return NTTime;
            case "LOCALTIME":
                return NTLocalTime;
            case "DATETIME":
                return NTDateTime;
            case "LOCALDATETIME":
                return NTLocalDateTime;
            case "DURATION":
                return NTDuration;
            case "POINT":
                return NTPoint;
            case "GEO":
                return NTGeometry;
            case "GEOMETRY":
                return NTGeometry;
            case "STRING":
                return NTString;
            case "TEXT":
                return NTString;
            default:
                return NTString;
        }
    }
}
