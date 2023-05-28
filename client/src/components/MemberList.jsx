import PropTypes from "prop-types";

const MemberList = ({ data }) => {
  if (typeof data === "undefined" || typeof data.members === "undefined") {
    return <div>Loading...</div>;
  }

  return (
    <div>
      {data.members.map((member, index) => (
        <p key={index}>{member}</p>
      ))}
    </div>
  );
};

MemberList.propTypes = {
  data: PropTypes.shape({
    members: PropTypes.array,
  }),
};

export default MemberList;
