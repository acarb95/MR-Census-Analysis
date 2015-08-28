all: compile
	@echo -e '[INFO] Done!'
clean:
	@echo -e '[INFO] Cleaning Up...'
	@-rm -rf *.class *~ Carbonari_Amanda_ASG3_PC.tar hw3.jar Objects/*.java
compile:
	@echo -e '[INFO] Compiling the Source...'
	@${HADOOP_HOME}/bin/hadoop com.sun.tools.javac.Main *.java Objects/*.java
	@echo -e '[INFO] Generating jar...'
	@-jar cf hw3.jar *.class Objects/*.class
package: 
	@echo -e '[INFO] Packaging the source...'
	@-tar cvf Carbonari_Amanda_ASG3_PC.tar *.java Makefile Objects/*.java README
