package org.example.poc.migration.transform;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * UT-6 — Binding enforcement of NFR-4 (architecture §5.1, CLAUDE.md invariant 1).
 *
 * Classes under {@code org.example.poc.migration.transform..} execute inside Spark
 * flatMap / mapPartitions closures. They are only allowed to touch RFile.newReader() /
 * RFile.newWriter() and Hadoop FileSystem. Any reference to the Accumulo client API
 * silently breaks the bypass guarantee that justifies the whole PoC, so the build must
 * fail the moment one appears.
 *
 * Vacuously passes today (no classes in transform/ yet) — it starts biting at Phase 3.
 */
@AnalyzeClasses(
    packages = "org.example.poc.migration.transform",
    importOptions = ImportOption.DoNotIncludeJars.class
)
class ClientBypassArchTest {

    @ArchTest
    static final ArchRule transform_must_not_depend_on_accumulo_client_packages =
        noClasses()
            .should().dependOnClassesThat().resideInAnyPackage(
                "org.apache.accumulo.core.client..",
                "org.apache.accumulo.core.clientImpl.."
            )
            .because(
                "NFR-4: transform/ runs inside Spark executors and must not reach the "
                + "Accumulo cluster — only RFile.newReader()/RFile.newWriter() are allowed."
            )
            .allowEmptyShould(true);

    @ArchTest
    static final ArchRule transform_must_not_use_named_client_types =
        noClasses()
            .should().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.Scanner")
            .orShould().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.BatchScanner")
            .orShould().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.BatchWriter")
            .orShould().dependOnClassesThat().haveFullyQualifiedName(
                "org.apache.accumulo.core.client.AccumuloClient")
            .because(
                "Defense-in-depth: the four client types explicitly named in NFR-4 / AC-6 "
                + "are banned by name, in case Accumulo moves them to a different package."
            )
            .allowEmptyShould(true);
}
