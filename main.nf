def helpMessage() {
    log.info"""
    =======================================================
                                              ,--./,-.
              ___     __   __   __   ___     /,-._.--~\'
        |\\ | |__  __ /  ` /  \\ |__) |__         }  {
        | \\| |       \\__, \\__/ |  \\ |___     \\`-._,-`-,
                                              `._,._,\'
     eQTL-Catalogue/genimpute v${workflow.manifest.version}
    =======================================================
    Usage:
    The typical command for running the pipeline is as follows:
    nextflow run main.nf -profile eqtl_catalogue -resume\
        --bfile /gpfs/hpc/projects/genomic_references/CEDAR/genotypes/PLINK_100718_1018/CEDAR\
        --harmonise_genotypes true\
        --output_name CEDAR_GRCh37_genotyped\
        --outdir CEDAR

    Mandatory arguments:
      --bfile                       Path to the TSV file containing pipeline inputs (VCF, expression matrix, metadata)
      --output_name                 Prefix for the output files

    Genotype harmonisation & QC:
      --harmonise_genotypes         Run GenotypeHarmonizer on the raw genotypes to correct flipped/swapped alleles (default: true)
      --ref_panel                   Reference panel used by GenotypeHarmonizer. Ideally should match the reference panel used for imputation.
      --ref_genome                  Reference genome fasta file for the raw genotypes (typically GRCh37).

    Phasing & Imputation:
      --eagle_genetic_map           Eagle genetic map file
      --eagle_phasing_reference     Phasing reference panel for Eagle (typically 1000 Genomes Phase 3)
      --minimac_imputation_reference Imputation reference panel for Minimac4 in M3VCF format (typically 1000 Genomes Phase 3)
      --r2_thresh                   Imputation quality score threshold for filtering poorly imputed variants

    CrossMap.py:
      --target_ref                  Reference genome fasta file for the target genome assembly (e.g. GRCh38)
      --chain_file                  Chain file to translate genomic cooridnates from the source assembly to target assembly
    
    Other options:
      --outdir                      The output directory where the results will be saved
      --email                       Set this parameter to your e-mail address to get a summary e-mail with details of the run sent to you when the workflow exits
      -name                         Name for the pipeline run. If not specified, Nextflow will automatically generate a random mnemonic.
    
    AWSBatch options:
      --awsqueue                    The AWSBatch JobQueue that needs to be set when running on AWSBatch
      --awsregion                   The AWS Region for your AWS Batch job to run on
    """.stripIndent()
}

// Show help emssage
if (params.help){
    helpMessage()
    exit 0
}

// Has the run name been specified by the user?
//  this has the bonus effect of catching both -name and --name
custom_runName = params.name
if( !(workflow.runName ==~ /[a-z]+_[a-z]+/) ){
  custom_runName = workflow.runName
}


// Define input channels
Channel
    .fromPath(params.ref_genome)
    .ifEmpty { exit 1, "Reference genome fasta file not found: ${params.ref_genome}" } 
    .set { ref_genome_ch }

Channel
    .from(params.bfile)
    .map { study -> [file("${study}.bed"), file("${study}.bim"), file("${study}.fam")]}
    .set { bfile_ch }

Channel
    .from(params.ref_panel)
    .map { ref -> [file("${ref}.vcf.gz"), file("${ref}.vcf.gz.tbi")]}
    .into { ref_panel_harmonise_genotypes; ref_panel_vcf_fixref } 

Channel
    .fromPath( "${params.eagle_phasing_reference}*" )
    .ifEmpty { exit 1, "Eagle phasing reference not found: ${params.eagle_phasing_reference}" }
    .set { phasing_ref_ch }

Channel
    .fromPath( "${params.minimac_imputation_reference}*" )
    .ifEmpty { exit 1, "Minimac4 imputation reference not found: ${params.minimac_imputation_reference}" }
    .set { imputation_ref_ch }

Channel
    .fromPath(params.eagle_genetic_map)
    .ifEmpty { exit 1, "Eagle genetic map file not found: ${params.eagle_genetic_map}" } 
    .set { genetic_map_ch }

Channel
    .fromPath(params.chain_file)
    .ifEmpty { exit 1, "CrossMap.py chain file not found: ${params.chain_file}" } 
    .set { chain_file_ch }

Channel
    .fromPath(params.target_ref)
    .ifEmpty { exit 1, "CrossMap.py target reference genome file: ${params.target_ref}" } 
    .set { target_ref_ch }

// Header log info
log.info """=======================================================
                                          ,--./,-.
          ___     __   __   __   ___     /,-._.--~\'
    |\\ | |__  __ /  ` /  \\ |__) |__         }  {
    | \\| |       \\__, \\__/ |  \\ |___     \\`-._,-`-,
                                          `._,._,\'
eQTL-Catalogue/genimpute v${workflow.manifest.version}"
======================================================="""
def summary = [:]
summary['Pipeline Name']            = 'eQTL-Catalogue/genimpute'
summary['Pipeline Version']         = workflow.manifest.version
summary['Run Name']                 = custom_runName ?: workflow.runName
summary['PLINK bfile']              = params.bfile
summary['Reference genome']         = params.ref_genome
summary['Harmonise genotypes']      = params.harmonise_genotypes
summary['Harmonisation ref panel']  = params.ref_panel
summary['Eagle genetic map']        = params.eagle_genetic_map
summary['Eagle reference panel']    = params.eagle_phasing_reference
summary['Minimac4 reference panel'] = params.minimac_imputation_reference
summary['CrossMap reference genome'] = params.target_ref
summary['CrossMap chain file']      = params.chain_file
summary['R2 thresh']      = params.chain_file
summary['Max Memory']               = params.max_memory
summary['Max CPUs']                 = params.max_cpus
summary['Max Time']                 = params.max_time
summary['Output name']              = params.output_name
summary['Output dir']               = params.outdir
summary['Working dir']              = workflow.workDir
summary['Container Engine']         = workflow.containerEngine
if(workflow.containerEngine) summary['Container'] = workflow.container
summary['Current home']             = "$HOME"
summary['Current user']             = "$USER"
summary['Current path']             = "$PWD"
summary['Working dir']              = workflow.workDir
summary['Script dir']               = workflow.projectDir
summary['Config Profile']           = workflow.profile
if(workflow.profile == 'awsbatch'){
   summary['AWS Region']            = params.awsregion
   summary['AWS Queue']             = params.awsqueue
}
if(params.email) summary['E-mail Address'] = params.email
log.info summary.collect { k,v -> "${k.padRight(21)}: $v" }.join("\n")
log.info "========================================="


if( workflow.profile == 'awsbatch') {
  // AWSBatch sanity checking
  if (!params.awsqueue || !params.awsregion) exit 1, "Specify correct --awsqueue and --awsregion parameters on AWSBatch!"
  if (!workflow.workDir.startsWith('s3') || !params.outdir.startsWith('s3')) exit 1, "Specify S3 URLs for workDir and outdir parameters on AWSBatch!"
  // Check workDir/outdir paths to be S3 buckets if running on AWSBatch
  // related: https://github.com/nextflow-io/nextflow/issues/813
  if (!workflow.workDir.startsWith('s3:') || !params.outdir.startsWith('s3:')) exit 1, "Workdir or Outdir not on S3 - specify S3 Buckets for each to run on AWSBatch!"
}
process impute_sex{
    input:
    tuple file(study_name_bed), file(study_name_bim), file(study_name_fam) from bfile_ch
    
    output:
    tuple file("CEDAR_chr23_noHET.bed"), file("CEDAR_chr23_noHET.bim"), file("CEDAR_chr23_noHET.fam") into sex_imputed,harmonize_input_ch
    
    script:
    """
    plink2 --bfile ${study_name_bed.baseName} --impute-sex --make-bed --out CEDAR_sex_imputed
    plink2 --bfile CEDAR_sex_imputed --chr 23 --make-bed --out CEDAR_chr23
    plink2 --bfile CEDAR_chr23 --split-x b37 no-fail --make-bed --out CEDAR_split_x
    plink2 --bfile CEDAR_split_x --chr 23 --make-bed --out CEDAR_chr23_noPAR
    plink2 --bfile CEDAR_chr23_noPAR --make-bed --set-hh-missing --out CEDAR_chr23_noHET
    
    """
}

process extract_female_samples{
    input:
    tuple file("CEDAR_chr23_noHET.bed"), file("CEDAR_chr23_noHET.bim"), file("CEDAR_chr23_noHET.fam") into sex_imputed
    output:
    file(“CEDAR_females.txt”) into female_sample_list_ch
    script:
    """
    plink2 --bfile CEDAR_chr23_noHET --filter-females --make-bed --out CEDAR_females
    cut -f1 -d' ' CEDAR_females.fam > CEDAR_females.txt
    """
 }
 process genotype_harmonizer{
    input:
    tuple file(bed), file(bim), file(fam) from harmonize_input_ch
    set file(vcf_file), file(vcf_file_index) from ref_panel_harmonise_genotypes.collect()

    output:
    
    tuple file("CEDAR_chr23_noHET_harmonized.bed"), file("CEDAR_chr23_noHET_harmonized.bim"), file("CEDAR_chr23_noHET_harmonized.fam") into harmonised_genotypes
   
    script:
    """
    sed 's/^23/X/g' CEDAR_chr23_noHET.bim > CEDAR_chr23_noHET.new_bim
    mv CEDAR_chr23_noHET.new_bim CEDAR_chr23_noHET.bim
    
    module load java-1.8.0_40
    java -jar ~/software/GenotypeHarmonizer-1.4.20-SNAPSHOT/GenotypeHarmonizer.jar\
     --input CEDAR_chr23_noHET\
     --inputType PLINK_BED\
     --ref ${vcf_file.simpleName}\
     --refType VCF\
     --update-id\
     --output CEDAR_chr23_noHET_harmonized
    """
 }
 
 process process make_vcf{
    input:
    tuple file(bed), file(bim), file(fam) from harmonised_genotypes
    
    output:
    CEDAR_harmonised_chrX.fixref.vcf.gz into filter_vcf_input
    
    script:
    """
    plink2 --bfile ${CEDAR_chr23_noHET_harmonized.baseName} --recode vcf-iid --out CEDAR_chr23_noHET_harmonized
    printf '23\tX\n' > 23_to_x.tsv
    bcftools annotate --rename-chrs 23_to_x.tsv CEDAR_chr23_noHET_harmonized.vcf -Oz -o CEDAR_harmonised_chrX.vcf.gz
    bcftools filter -e "ALT='.'" CEDAR_harmonised_chrX.vcf.gz | bcftools filter -i 'F_MISSING < 0.05' -Oz -o CEDAR_harmonised_chrX.filtered.vcf.gz
    bcftools +fixref CEDAR_harmonised_chrX.filtered.vcf.gz -Oz -o CEDAR_harmonised_chrX.fixref.vcf.gz -- \
    -f /gpfs/hpc/projects/genomic_references/annotations/GRCh37/Homo_sapiens.GRCh37.dna.primary_assembly.fa \
    -i /gpfs/hpc/projects/genomic_references/1000G/GRCh37/1000G_GRCh37_variant_information.vcf.gz
    """
  }
