package apoc.help;


import org.objectweb.asm.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 11.04.16
 */
public class HelpScanner {

    final static Map<String,HelpResult> procedures = new TreeMap<>();

    static {
        new HelpScanner().scanAll();
    }
    public static final String JAR_NAME_PART = "apoc";

    void handleClass(InputStream in) throws IOException {
        MyClassVisitor cv = new MyClassVisitor();
        new ClassReader(in).accept(cv, 0);
    }

    public static Stream<HelpResult> find(String name, boolean searchText) {
        if (name==null) return Stream.empty();
        return procedures.entrySet().stream()
                .filter(e -> search(e.getKey(), name) || (searchText && search(e.getValue(), name)))
                .map(Map.Entry::getValue);
    }

    private static boolean search(String key, String name) {
        // A better algorithm may be needed here (regex or Pattern)
        if (key == null || name == null) return false;
        return key.toLowerCase().contains(name.toLowerCase());
    }

    private static boolean search(HelpResult value, String name) {
        if (value == null) return false;
        return search(value.text, name);
    }

    class MyClassVisitor extends ClassVisitor {
        public String className;
        public String packageName;

        MyClassVisitor() {
            super(Opcodes.ASM5);
        }

        @Override
        public void visit(int version,
                          int access, String name, String signature,
                          String superName, String[] interfaces) {
            className = name.replace('/', '.');
            packageName = className.substring(0,className.lastIndexOf('.'));
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
            return new MethodVisitor(api) {
                String descriptionText;
                boolean isProcedure;
                boolean performsWrites;
                String procedureName = packageName + "." + name;
                @Override
                public AnnotationVisitor visitAnnotation(String annotation, boolean b) {
//                    System.out.println("annotation = " + annotation);
                    switch (annotation) {
                        case "Lapoc/Description;":
//                            System.out.println("Annotation " + annotation);
                            return new AnnotationVisitor(api) {
                                @Override
                                public void visit(String name, Object value) {
//                                    System.out.println("name = " + name+" "+ value);
                                    if (Objects.equals(name, "value") && value!=null) {
                                        descriptionText = value.toString();
//                                        System.err.println("Description: "+ descriptionText);
                                    }
                                }
                            };
                        case "Lorg/neo4j/procedure/Procedure;":
//                            System.out.println("Procedure " + annotation);
                            isProcedure = true;
                            return new AnnotationVisitor(api) {
                                @Override
                                public void visit(String name, Object value) {
//                                    System.out.println("name = " + name+" "+ value);
                                    if (Objects.equals(name, "value") && value!=null) {
                                        procedureName = value.toString();
//                                        System.err.println("Procedure: "+ procedureName);
                                    }
                                }
                            };
                        case "Lorg/neo4j/procedure/PerformsWrites;": performsWrites = true;
                    }
                    return null;
                }

                @Override
                public void visitEnd() {
                    if (isProcedure) {
//                     System.err.printf("APOC: %s declares procedure %s writes %s desc %s%n", className, procedureName, performsWrites, descriptionText);
                     procedures.put(procedureName,new HelpResult(procedureName,descriptionText,performsWrites));
                    }
                }
            };
        }
    }

    List<URL> getRootUrls() {
        List<URL> result = new ArrayList<>();

        ClassLoader cl = HelpScanner.class.getClassLoader();
        while (cl != null) {
            if (cl instanceof URLClassLoader) {
                URL[] urls = ((URLClassLoader) cl).getURLs();
                result.addAll(Arrays.asList(urls));
            }
            cl = cl.getParent();
        }
        return result;
    }

    void scanAll() {
        try {
            for (URL url : getRootUrls()) {
                File f = new File(url.getPath());
                if (f.isDirectory()) {
                    visitFile(f);
                } else {
                    visitJar(url);
                }
            }
        } catch(Exception e) {
            System.err.println("APOC: Error scanning procedures " + e.getMessage());
        }
    }

    void visitFile(File f) throws IOException {
        if (f.isDirectory()) {
            final File[] children = f.listFiles();
            if (children != null) {
                for (File child : children) {
                    visitFile(child);
                }
            }
// we don't need to scan plain class files
        } else if (f.getName().endsWith(".class")) {
            try (FileInputStream in = new FileInputStream(f)) {
                handleClass(in);
            }
        } else {
            String fileName = f.getName();
            if (fileName.endsWith(".jar") && fileName.contains(JAR_NAME_PART)) {
                try (FileInputStream in = new FileInputStream(f)) {
                    visitJar(in);
                }
            }
        }
    }

    void visitJar(URL url) throws IOException {
        if (!url.getPath().contains(JAR_NAME_PART)) return;
        try (InputStream urlIn = url.openStream()) {
            visitJar(urlIn);
        }
    }

    private void visitJar(InputStream in) throws IOException {
        try (JarInputStream jarIn = new JarInputStream(in)) {
            JarEntry entry;
            while ((entry = jarIn.getNextJarEntry()) != null) {
                if (entry.getName().endsWith(".class")) {
                    handleClass(jarIn);
                }
            }
        }
    }
}
