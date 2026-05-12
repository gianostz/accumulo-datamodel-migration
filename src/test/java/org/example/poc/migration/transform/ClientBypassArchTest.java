package org.example.poc.migration.transform;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * UT-6 — Binding enforcement of NFR-4 (architecture §5.1, CLAUDE.md invariant 1).
 *
 * <p>Classes under {@code org.example.poc.migration.transform..} execute inside Spark
 * {@code flatMap} / {@code mapPartitions} closures. They are only allowed to touch the static
 * RFile read/write API and Hadoop {@code FileSystem}. Any reference to the Accumulo client
 * API that opens a cluster connection silently breaks the bypass guarantee that justifies
 * the whole PoC, so the build must fail the moment one appears.
 *
 * <h3>Carve-outs (deliberately narrow)</h3>
 * The PoC must use {@code RFile.newScanner()} / {@code RFile.newWriter()} (FR-3, NFR-4).
 * In Accumulo 2.1.x the public static-file API is exposed under
 * {@code org.apache.accumulo.core.client.rfile} and {@code RFile.newScanner().build()} returns
 * a {@code org.apache.accumulo.core.client.Scanner} bound to an RFile, not to a cluster.
 * The rule therefore exempts, and ONLY exempts:
 * <ul>
 *   <li>everything under {@code org.apache.accumulo.core.client.rfile..} (static file API)</li>
 *   <li>{@code org.apache.accumulo.core.client.Scanner} (return type of {@code newScanner().build()})</li>
 *   <li>{@code org.apache.accumulo.core.client.ScannerBase} (supertype reachable via Scanner)</li>
 * </ul>
 * The cluster-connecting types {@code BatchScanner}, {@code BatchWriter}, and
 * {@code AccumuloClient} remain banned by name as defense-in-depth.
 *
 * <p>If a future class in {@code transform/} legitimately needs another type, prefer wrapping
 * it in a non-{@code transform/} helper rather than widening this carve-out.
 */
@AnalyzeClasses(
    packages = "org.example.poc.migration.transform",
    importOptions = ImportOption.DoNotIncludeJars.class
)
class ClientBypassArchTest {

    /** Names exempted from the package ban — the precise static-file API surface (see Javadoc). */
    private static final DescribedPredicate<JavaClass> FORBIDDEN_CLIENT_TYPE =
        new DescribedPredicate<>(
            "in org.apache.accumulo.core.client.. / clientImpl.., except client.rfile.. "
            + "and except client.Scanner / client.ScannerBase (RFile static-file API carve-out)"
        ) {
            @Override
            public boolean test(JavaClass javaClass) {
                String n = javaClass.getName();
                if (n.startsWith("org.apache.accumulo.core.client.rfile.")) {
                    return false;
                }
                if (n.equals("org.apache.accumulo.core.client.Scanner")
                        || n.equals("org.apache.accumulo.core.client.ScannerBase")) {
                    return false;
                }
                return n.startsWith("org.apache.accumulo.core.client.")
                        || n.startsWith("org.apache.accumulo.core.clientImpl.");
            }
        };

    @ArchTest
    static final ArchRule transform_must_not_depend_on_accumulo_client_packages =
        noClasses()
            .should().dependOnClassesThat(FORBIDDEN_CLIENT_TYPE)
            .because(
                "NFR-4 / AC-6: transform/ runs inside Spark executors and must not reach the "
                + "Accumulo cluster. Only the static RFile API (client.rfile..) and its Scanner "
                + "return type are exempt — see ClientBypassArchTest javadoc."
            )
            .allowEmptyShould(true);

    @ArchTest
    static final ArchRule transform_must_not_use_cluster_client_types =
        noClasses()
            .should().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.BatchScanner")
            .orShould().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.BatchWriter")
            .orShould().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.AccumuloClient")
            .because(
                "Defense-in-depth: the three cluster-connecting client types from NFR-4 / AC-6 "
                + "are banned by FQN, in case Accumulo moves them to a different package. "
                + "Scanner is intentionally absent — see carve-out in ClientBypassArchTest javadoc."
            )
            .allowEmptyShould(true);
}
