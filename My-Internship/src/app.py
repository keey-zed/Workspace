import streamlit as st
import numpy as np
import os

# Setting seed for reproducibility
np.random.seed(1234)
PYTHONHASHSEED = 0

from keras.models import load_model

# define path to save model
model_path = 'C:/Users/"840 G6"/projects/SI/workspace/My-Internship/Output/binary_model.h5'

sequence_length = 50
nb_features = 25

# Streamlit GUI
def load_trained_model():
    if os.path.isfile(model_path):
        return load_model(model_path)
    else:
        return None

def make_prediction(model, uploaded_file):
    # Check if a file is uploaded
    if uploaded_file is not None:
        try:
            # Preprocess user input from the file
            user_input = preprocess_file_input(uploaded_file)

            if user_input is not None:
                # Make predictions using the loaded model
                prediction = model.predict(user_input.reshape(1, sequence_length, nb_features))
                return prediction
            else:
                st.warning("Please upload a valid text file before making a prediction.")
                return None
        except Exception as e:
            st.error(f"Error making prediction: {e}")
            return None
    else:
        st.warning("Please upload a text file before making a prediction.")
        return None

def preprocess_file_input(uploaded_file):
    try:
        if isinstance(uploaded_file, np.ndarray):
            # If uploaded_file is a NumPy array
            user_input = uploaded_file
        else:
            # If uploaded_file is a file object
            # Read data from the uploaded file
            content = uploaded_file.read().decode("utf-8")

            # Process the content and split values by space
            data = [list(map(float, line.split())) for line in content.splitlines()]

            # Convert the list of lists to a numpy array
            user_input = np.array(data)

        # Assuming each data point has a shape of (sequence_length, nb_features)
        user_input = user_input.reshape(sequence_length, nb_features)

        return user_input

    except Exception as e:
        st.error(f"Error processing file content: {e}")
        return None

def main():
    st.title("Machinery Health Monitoring")

    # Load trained model
    model = load_trained_model()

    if model is None:
        st.warning("Model not found. Please train the model first.")
        return

    # Sidebar with user input
    st.sidebar.header("User Input Parameters")

    # Option to upload a text file
    uploaded_file = st.sidebar.file_uploader("Upload a text file:", type=["txt"])

    if uploaded_file is not None:
        # Display the uploaded file content
        st.sidebar.text("Uploaded File Content:")
        st.sidebar.text(uploaded_file.getvalue().decode("utf-8"))

    # Option to make predictions based on user input from the file
    if st.button("Make Prediction"):
        if uploaded_file:
            # Preprocess user input from the file
            user_input = preprocess_file_input(uploaded_file)

            if user_input is not None:
                # Make predictions
                prediction = make_prediction(model, user_input)

                # Display the prediction
                st.subheader("Prediction:")
                st.write(prediction)
        else:
            st.warning("Please upload a text file before making a prediction.")


if __name__ == "__main__":
    main()
