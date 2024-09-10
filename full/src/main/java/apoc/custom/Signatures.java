/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.custom;

import static apoc.custom.CypherProceduresUtil.MAP_RESULT_TYPE;
import static apoc.custom.CypherProceduresUtil.getBaseType;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.*;

import apoc.util.JsonUtil;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.*;
import org.neo4j.internal.kernel.api.procs.*;
import org.neo4j.procedure.Mode;

public class Signatures {

    public static final String SIGNATURE_SYNTAX_ERROR = "Syntax error(s) in signature definition %s. "
            + "\nNote that procedure/function name, possible map keys, input and output names must have at least 2 character:\n";
    private static final String MAP_RESULT_TYPE = "MAPRESULT";
    private final String prefix;

    public Signatures(String prefix) {
        this.prefix = prefix;
    }

    public Signatures() {
        this(null);
    }

    public SignatureParser.ProcedureContext parseProcedure(String procedureSignatureText) {
        List<String> errors = new ArrayList<>();
        SignatureParser signatureParser = parse(procedureSignatureText, errors);
        final SignatureParser.ProcedureContext signatureParsed = signatureParser.procedure();
        checkSignatureSyntax(procedureSignatureText, errors);
        return signatureParsed;
    }

    public SignatureParser.FunctionContext parseFunction(String functionSignatureText) {
        List<String> errors = new ArrayList<>();
        SignatureParser signatureParser = parse(functionSignatureText, errors);
        final SignatureParser.FunctionContext signatureParsed = signatureParser.function();
        checkSignatureSyntax(functionSignatureText, errors);
        return signatureParsed;
    }

    private void checkSignatureSyntax(String functionSignatureText, List<String> errors) {
        if (!errors.isEmpty()) {
            throw new RuntimeException(
                    String.format(SIGNATURE_SYNTAX_ERROR, functionSignatureText) + String.join("\n", errors));
        }
    }

    private SignatureParser parse(String signatureText, List<String> errors) {
        CodePointCharStream inputStream = CharStreams.fromString(signatureText);
        SignatureLexer signatureLexer = new SignatureLexer(inputStream);
        CommonTokenStream commonTokenStream = new CommonTokenStream(signatureLexer);
        SignatureParser signatureParser = new SignatureParser(commonTokenStream);
        signatureParser.addErrorListener(new BaseErrorListener() {
            public void syntaxError(
                    Recognizer<?, ?> recognizer,
                    Object offendingSymbol,
                    int line,
                    int charPositionInLine,
                    String msg,
                    RecognitionException e) {
                errors.add("line " + line + ":" + charPositionInLine + " " + msg);
            }
        });
        return signatureParser;
    }

    public ProcedureSignature toProcedureSignature(SignatureParser.ProcedureContext signature) {
        return toProcedureSignature(signature, null, Mode.DEFAULT);
    }

    public static FieldSignature getInputField(
            String name, Neo4jTypes.AnyType type, DefaultParameterValue defaultValue) {
        if (defaultValue == null) {
            return FieldSignature.inputField(name, type);
        }
        return FieldSignature.inputField(name, type, defaultValue);
    }

    public ProcedureSignature toProcedureSignature(
            SignatureParser.ProcedureContext signature, String description, Mode mode) {
        QualifiedName name = new QualifiedName(namespace(signature.namespace()), name(signature.name()));
        List<FieldSignature> outputSignature = signature.results().empty() != null
                ? ProcedureSignature.VOID
                : signature.results().result().stream()
                        .map(p -> FieldSignature.outputField(name(p.name()), type(p.type())))
                        .collect(Collectors.toList());
        // todo deprecated + default value
        List<FieldSignature> inputSignatures = signature.parameter().stream()
                .map(p -> getInputField(name(p.name()), type(p.type()), defaultValue(p.defaultValue(), type(p.type()))))
                .collect(Collectors.toList());
        boolean admin = false;
        String deprecated = "";
        String[] allowed = new String[0];
        String warning = null; // "todo warning";
        boolean eager = false;
        boolean caseInsensitive = true;
        return createProcedureSignature(
                name,
                inputSignatures,
                outputSignature,
                mode,
                admin,
                deprecated,
                allowed,
                description,
                warning,
                eager,
                caseInsensitive,
                false,
                false,
                false);
    }

    public List<String> namespace(SignatureParser.NamespaceContext namespaceContext) {
        List<String> parsed = namespaceContext == null
                ? Collections.emptyList()
                : namespaceContext.name().stream().map(this::name).collect(Collectors.toList());
        if (prefix == null) {
            return parsed;
        } else {
            ArrayList<String> namespace = new ArrayList<>();
            namespace.add(prefix);
            namespace.addAll(parsed);
            return namespace;
        }
    }

    public UserFunctionSignature toFunctionSignature(SignatureParser.FunctionContext signature, String description) {
        QualifiedName name = new QualifiedName(namespace(signature.namespace()), name(signature.name()));

        Neo4jTypes.AnyType type = type(signature.type());

        List<FieldSignature> inputSignatures = signature.parameter().stream()
                .map(p -> getInputField(name(p.name()), type(p.type()), defaultValue(p.defaultValue(), type(p.type()))))
                .collect(Collectors.toList());

        String deprecated = "";
        String[] allowed = new String[0];
        boolean caseInsensitive = true;
        return new UserFunctionSignature(
                name, inputSignatures, type, deprecated, allowed, description, "apoc.custom", caseInsensitive);
    }

    private DefaultParameterValue defaultValue(
            SignatureParser.DefaultValueContext defaultValue, Neo4jTypes.AnyType type) {
        // pass a default value = null into the signature string is not equal to having `defaultValue == null`
        // the defaultValue is null only when we don't pass the default value part
        if (defaultValue == null) return null;
        SignatureParser.ValueContext v = defaultValue.value();
        if (v.nullValue() != null) return DefaultParameterValue.nullValue(type);
        if (v.boolValue() != null)
            return DefaultParameterValue.ntBoolean(
                    Boolean.parseBoolean(v.boolValue().getText()));
        final SignatureParser.StringValueContext stringCxt = v.stringValue();
        if (stringCxt != null) {

            String text = stringCxt.getText();
            if (stringCxt.SINGLE_QUOTED_STRING_VALUE() != null || stringCxt.QUOTED_STRING_VALUE() != null) {
                text = text.substring(1, text.length() - 1);
            }
            return DefaultParameterValue.ntString(text);
        }
        if (v.INT_VALUE() != null) {
            final String text = v.INT_VALUE().getText();
            return getDefaultParameterValue(type, text, () -> DefaultParameterValue.ntInteger(Long.parseLong(text)));
        }
        if (v.FLOAT_VALUE() != null) {
            final String text = v.FLOAT_VALUE().getText();
            return getDefaultParameterValue(type, text, () -> DefaultParameterValue.ntFloat(Double.parseDouble(text)));
        }
        if (v.mapValue() != null) {
            Map map = JsonUtil.parse(v.mapValue().getText(), null, Map.class);
            return DefaultParameterValue.ntMap(map);
        }
        if (v.listValue() != null) {
            List<?> list = JsonUtil.parse(v.listValue().getText(), null, List.class);
            final AnyType inner = ((ListType) type).innerType();
            if (inner instanceof TextType) {
                list = list.stream().map(String::valueOf).collect(Collectors.toList());
            }
            return DefaultParameterValue.ntList(list, inner);
        }
        return DefaultParameterValue.nullValue(type);
    }

    private DefaultParameterValue getDefaultParameterValue(
            AnyType type, String text, Supplier<DefaultParameterValue> fun) {
        // to differentiate e.g. null (nullValue) from null as a plain string, or 1 (integer) from 1 as a plain text
        // we have to obtain the actual data type from type.
        // Otherwise we could we can remove the possibility of having plainText string and explicit them via
        // quotes/double-quotes
        // or document that null/numbers/boolean as a plain string are not possible.
        return type instanceof TextType ? DefaultParameterValue.ntString(text) : fun.get();
    }

    public String name(SignatureParser.NameContext ns) {
        if (ns == null)
            throw new IllegalStateException("Unsupported procedure name, the procedure must have at least two chars");
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

    public boolean isMapResult(SignatureParser.FunctionContext functionContext) {
        final SignatureParser.TypeContext outputType = functionContext.type();
        return outputType.getText().contains(MAP_RESULT_TYPE);
    }

    private Neo4jTypes.AnyType type(SignatureParser.Opt_typeContext opt_type) {
        return getBaseType(opt_type.base_type().getText());
    }

    public UserFunctionSignature asFunctionSignature(String signature, String description) {
        SignatureParser.FunctionContext functionContext = parseFunction(signature);
        return toFunctionSignature(functionContext, description);
    }

    public ProcedureSignature asProcedureSignature(String signature, String description, Mode mode) {
        SignatureParser.ProcedureContext ctx = parseProcedure(signature);
        return toProcedureSignature(ctx, description, mode);
    }

    public static ProcedureSignature createProcedureSignature(
            QualifiedName name,
            List<FieldSignature> inputSignature,
            List<FieldSignature> outputSignature,
            Mode mode,
            boolean admin,
            String deprecated,
            String[] allowed,
            String description,
            String warning,
            boolean eager,
            boolean caseInsensitive,
            boolean systemProcedure,
            boolean internal,
            boolean allowExpiredCredentials) {
        try {
            // in Neo4j 4.0.5 org.neo4j.internal.kernel.api.procs.ProcedureSignature
            // changed the signature adding a boolean at the end and without leaving the old signature
            // in order to maintain the backwards compatibility with version prior to 4.0.5 we use the
            // reflection to create a new instance of the class
            // in Neo4j 4.3 another boolean was added
            final Class<?> clazz = Class.forName("org.neo4j.internal.kernel.api.procs.ProcedureSignature");
            final Constructor<?>[] constructors = clazz.getConstructors();
            for (int i = 0; i < constructors.length; i++) {
                final Constructor<?> constructor = constructors[i];
                switch (constructor.getParameterCount()) {
                    case 14:
                        return (ProcedureSignature) constructor.newInstance(
                                name,
                                inputSignature,
                                outputSignature,
                                mode,
                                admin,
                                deprecated,
                                allowed,
                                description,
                                warning,
                                eager,
                                caseInsensitive,
                                systemProcedure,
                                internal,
                                allowExpiredCredentials);
                    case 13:
                        return (ProcedureSignature) constructor.newInstance(
                                name,
                                inputSignature,
                                outputSignature,
                                mode,
                                admin,
                                deprecated,
                                allowed,
                                description,
                                warning,
                                eager,
                                caseInsensitive,
                                systemProcedure,
                                internal);
                    case 12:
                        return (ProcedureSignature) constructor.newInstance(
                                name,
                                inputSignature,
                                outputSignature,
                                mode,
                                admin,
                                deprecated,
                                allowed,
                                description,
                                warning,
                                eager,
                                caseInsensitive,
                                systemProcedure);
                }
            }
            throw new RuntimeException(
                    "Constructor of org.neo4j.internal.kernel.api.procs.ProcedureSignature not found");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
