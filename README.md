# Twitter Sentiment Analysis in Scala Project Repository


### **Project Title**  
#### Real-Time Twitter Sentiment Analysis using Apache Spark in Scala

### **Objective**  
The project aims to implement a scalable and efficient real-time Twitter sentiment analysis system using **Apache Spark** and its supporting libraries. The system processes, analyzes, and visualizes sentiment trends from Twitter data streams.

---

### **Key Features**
- **Technologies Used**:  
  - Apache Spark (Streaming, MLlib, SQL, RDD)  
  - Scala  
  - MongoDB Atlas  
  - Power BI  
- **Core Functionalities**:  
  - Simulated Real-time data processing with Spark Streaming.  
  - Sentiment classification using machine learning models (Logistic Regression, Random Forest, Multilayer Perceptron).  
  - Data visualization and dashboard creation using Power BI.  

---

### **Setup and Configuration**  
1. Install Java 11, Scala 2.13, and Apache Spark 3.4 with Hadoop 3.3.6.  
2. Configure `SPARK_HOME` and `HADOOP_HOME` environment variables.  
3. Start a Spark master and worker for local or cluster mode processing.

---

### **Workflow**  
1. **Data Collection**: Simulated streaming of Twitter data.  
2. **Preprocessing**: Cleaning, tokenization, stopword removal, vectorization using TF-IDF, and label encoding.  
3. **Modeling**:  
   - Models trained:  
     - Logistic Regression: 53.6% accuracy  
     - Random Forest: 55.3% accuracy  
     - Multilayer Perceptron: 67.9% accuracy (best performance)  
4. **Storage and Visualization**:  
   - Data and model metrics stored in MongoDB Atlas.  
   - Sentiment trends visualized via Power BI dashboards.

---

### **Results**
- High performance with the MLP model for sentiment prediction.  
- Interactive dashboards displaying sentiment distributions (positive, negative, neutral).  

---

### **Contributors**  
- Ayush Bajracharya  
- Ashwini Choudhary  
- Aya Amarass  
- Asma Hajri  
- Amey Pathare  
