
FinalProject.jar: FinalProject.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/FinalProject-all.jar FinalProject.jar

run: FinalProject.jar
	rm -rf output
	hadoop jar FinalProject.jar labeled.csv.bz2 output output/val 10 unlabelled output_final output_val unlabeled unlabeled.csv.bz2

clean:
	rm -rf build FinalProject.jar output .gradle model output_val
